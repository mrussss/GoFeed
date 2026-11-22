package feed

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http/httptest"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"feedsystem_video_go/internal/social"
	"feedsystem_video_go/internal/video"

	rediscache "feedsystem_video_go/internal/middleware/redis"
	"github.com/gin-gonic/gin"
	goredis "github.com/redis/go-redis/v9"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type scaleProbe struct {
	pipelineCalls, pipelineNanos, pipelineTimeouts, pipelineErrors atomic.Int64
	selects, completeSQL                                           atomic.Int64
}

func (*scaleProbe) DialHook(next goredis.DialHook) goredis.DialHook          { return next }
func (*scaleProbe) ProcessHook(next goredis.ProcessHook) goredis.ProcessHook { return next }
func (p *scaleProbe) ProcessPipelineHook(next goredis.ProcessPipelineHook) goredis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []goredis.Cmder) error {
		start := time.Now()
		err := next(ctx, cmds)
		if len(cmds) > 0 && cmds[0].Name() == "zrevrangebylex" {
			p.pipelineCalls.Add(1)
			p.pipelineNanos.Add(time.Since(start).Nanoseconds())
			if err != nil {
				p.pipelineErrors.Add(1)
				var networkError net.Error
				if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &networkError) && networkError.Timeout()) {
					p.pipelineTimeouts.Add(1)
				}
			}
		}
		return err
	}
}

type scaleLogger struct {
	logger.Interface
	probe *scaleProbe
}

func (l scaleLogger) Trace(_ context.Context, _ time.Time, query func() (string, int64), _ error) {
	sql, _ := query()
	if strings.HasPrefix(sql, "SELECT ") {
		l.probe.selects.Add(1)
		if strings.Contains(sql, "FROM `videos`") && strings.Contains(sql, "ORDER BY create_time DESC, id DESC") {
			l.probe.completeSQL.Add(1)
		}
	}
}

func TestFollowingScaleEvidence(t *testing.T) {
	if os.Getenv("GOFEED_FOLLOWING_SCALE") != "1" {
		t.Skip("set GOFEED_FOLLOWING_SCALE=1 to run six scale rounds")
	}
	db := readTestMySQL(t)
	pool, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(16)
	pool.SetMaxIdleConns(8)
	raw := goredis.NewClient(&goredis.Options{Addr: os.Getenv("C_TEST_REDIS_ADDR"), DB: 15, ContextTimeoutEnabled: true})
	defer raw.Close()
	ctx := context.Background()
	if err := raw.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	cache := rediscache.NewClient(raw, fmt.Sprintf("following-scale:%d:", time.Now().UnixNano()))
	defer cache.DelByPattern(ctx, cache.Key("*"))
	const firstAuthor, firstVideo, firstViewer = 100000, 200000, 110001
	stamp := time.UnixMilli(1_794_012_300_123).UTC()
	sizes := []int{500, 5000, 10000}
	var videos []video.Video
	var links []social.Social
	for i := 0; i < sizes[len(sizes)-1]; i++ {
		author := uint(firstAuthor + i)
		for j := 0; j < 3; j++ {
			videos = append(videos, video.Video{ID: uint(firstVideo + 3*i + j), AuthorID: author, CreateTime: stamp.Add(time.Duration(i) * time.Millisecond)})
		}
		for index, n := range sizes {
			if i < n {
				links = append(links, social.Social{FollowerID: uint(firstViewer + index), VloggerID: author})
			}
		}
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.CreateInBatches(videos, 500).Error; err != nil {
			return err
		}
		return tx.CreateInBatches(links, 500).Error
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Where("follower_id BETWEEN ? AND ?", firstViewer, firstViewer+len(sizes)-1).Delete(&social.Social{}).Error; err != nil {
			t.Error(err)
		}
		if err := db.Where("id BETWEEN ? AND ?", firstVideo, firstVideo+len(videos)-1).Delete(&video.Video{}).Error; err != nil {
			t.Error(err)
		}
	}()

	for i := 0; i < len(videos); i += 300 {
		end := min(i+300, len(videos))
		_, err := raw.Pipelined(ctx, func(pipe goredis.Pipeliner) error {
			for _, v := range videos[i:end] {
				for index, n := range sizes {
					if int(v.AuthorID)-firstAuthor >= n {
						continue
					}
					if v.AuthorID%2 == 0 {
						pipe.ZAdd(ctx, cache.Key("feed:inbox:%d", firstViewer+index), timelineZ(t, v.CreateTime, v.ID))
					} else {
						pipe.ZAdd(ctx, cache.Key("feed:author_outbox:%d", v.AuthorID), timelineZ(t, v.CreateTime, v.ID))
					}
				}
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for index, authors := range sizes {
		viewer := uint(firstViewer + index)
		// Match the production Inbox size while preserving all videos that can
		// appear in these first three pages.
		if err := raw.ZRemRangeByRank(ctx, cache.Key("feed:inbox:%d", viewer), 0, -1001).Err(); err != nil {
			t.Fatal(err)
		}
		oracle, err := followingEvidenceOracle(db, viewer)
		if err != nil || len(oracle) != 3*authors {
			t.Fatalf("oracle returned %d rows: %v", len(oracle), err)
		}
		for _, vus := range []int{1, 10} {
			if !t.Run(fmt.Sprintf("%d/%d", authors, vus), func(t *testing.T) {
				runFollowingScaleEvidence(t, db, cache, raw.Options(), viewer, oracle, authors, vus)
			}) {
				return
			}
		}
	}
}

func runFollowingScaleEvidence(t *testing.T, db *gorm.DB, originalCache *rediscache.Client, options *goredis.Options, viewer uint, oracle []uint, authors, vus int) {
	const pageSize = 10
	const measureFor = 8 * time.Second
	probe := new(scaleProbe)
	raw := goredis.NewClient(options)
	defer raw.Close()
	raw.AddHook(probe)
	cache := rediscache.NewClient(raw, originalCache.Key(""))
	instrumented := db.Session(&gorm.Session{Logger: scaleLogger{Interface: logger.Default.LogMode(logger.Silent), probe: probe}, PrepareStmt: true})
	service := NewFeedService(NewFeedRepository(instrumented), video.NewLikeRepository(instrumented), social.NewSocialRepository(instrumented), cache)
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/feed/listByFollowing", func(c *gin.Context) {
		c.Set("accountID", viewer)
		NewFeedHandler(service).ListByFollowing(c)
	})
	type observed struct {
		status int
		ms     float64
		page   ListByFollowingResponse
		valid  bool
	}
	request := func(body string, expected []uint) observed {
		req := httptest.NewRequest("POST", "/feed/listByFollowing", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		rec := httptest.NewRecorder()
		start := time.Now()
		router.ServeHTTP(rec, req)
		r := observed{status: rec.Code, ms: float64(time.Since(start)) / float64(time.Millisecond)}
		if rec.Code == 200 && json.Unmarshal(rec.Body.Bytes(), &r.page) == nil {
			r.valid = slices.Equal(feedIDs(r.page.VideoList), expected) && r.page.HasMore && r.page.NextIDBefore == expected[len(expected)-1]
			if r.valid {
				var stamp time.Time
				stamp = time.UnixMilli(1_794_012_300_123 + int64((expected[len(expected)-1]-200000)/3))
				r.valid = r.page.NextTime == stamp.UnixMilli()
			}
		}
		return r
	}
	body := `{"limit":10}`
	for offset := 0; offset < 3*pageSize; offset += pageSize {
		result := request(body, oracle[offset:offset+pageSize])
		if result.status != 200 || !result.valid {
			t.Fatalf("oracle preflight page %d: %+v", offset/pageSize+1, result)
		}
		body = fmt.Sprintf(`{"limit":10,"latest_time":%d,"id_before":%d}`, result.page.NextTime, result.page.NextIDBefore)
	}
	for _, counter := range []*atomic.Int64{&probe.pipelineCalls, &probe.pipelineNanos, &probe.pipelineTimeouts, &probe.pipelineErrors, &probe.completeSQL, &probe.selects} {
		counter.Store(0)
	}
	type tally struct {
		durations              []float64
		failed, contractErrors int
	}
	perWorker := make([]tally, vus)
	var wait sync.WaitGroup
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	cpuBefore := scaleCPU(t)
	var rss atomic.Int64
	done := make(chan struct{})
	sampled := make(chan struct{})
	go func() {
		defer close(sampled)
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			rss.Store(max(rss.Load(), scaleRSS()))
			select {
			case <-done:
				return
			case <-ticker.C:
			}
		}
	}()
	start := time.Now()
	deadline := start.Add(measureFor)
	for id := range perWorker {
		wait.Add(1)
		go func() {
			defer wait.Done()
			current := &perWorker[id]
			for time.Now().Before(deadline) {
				got := request(`{"limit":10}`, oracle[:pageSize])
				current.durations = append(current.durations, got.ms)
				if got.status != 200 {
					current.failed++
				} else if !got.valid {
					current.contractErrors++
				}
			}
		}()
	}
	wait.Wait()
	elapsed := time.Since(start)
	cpu := scaleCPU(t) - cpuBefore
	runtime.ReadMemStats(&after)
	close(done)
	<-sampled
	var durations []float64
	var failed, contractErrors int
	for _, entry := range perWorker {
		durations = append(durations, entry.durations...)
		failed += entry.failed
		contractErrors += entry.contractErrors
	}
	slices.Sort(durations)
	n := float64(len(durations))
	pipelines := float64(probe.pipelineCalls.Load())
	result, err := json.Marshal(map[string]any{
		"authors": authors, "vu": vus, "round": 1, "elapsed_s": elapsed.Seconds(),
		"requests": len(durations), "http_errors": failed, "contract_errors": contractErrors,
		"p95_ms": durations[(len(durations)*95+99)/100-1], "p99_ms": durations[(len(durations)*99+99)/100-1],
		"success_per_s": (n - float64(failed+contractErrors)) / elapsed.Seconds(), "pipeline_calls": pipelines,
		"pipeline_mean_ms":  float64(probe.pipelineNanos.Load()) / float64(time.Millisecond) / pipelines,
		"pipeline_timeouts": probe.pipelineTimeouts.Load(), "pipeline_errors": probe.pipelineErrors.Load(),
		"full_mysql_sql": probe.completeSQL.Load(), "sql_per_request": float64(probe.selects.Load()) / n,
		"api_process_cpu_pct": 100 * cpu / elapsed.Seconds(), "sampled_peak_rss_kib": rss.Load(),
		"allocated_kib_per_request": float64(after.TotalAlloc-before.TotalAlloc) / 1024 / n,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("SCALE_RESULT %s", result)
	if contractErrors != 0 {
		t.Fatalf("%d HTTP 200 responses differed from the independent SQL oracle", contractErrors)
	}
}

func scaleCPU(t *testing.T) float64 {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return float64(usage.Utime.Sec+usage.Stime.Sec) + float64(usage.Utime.Usec+usage.Stime.Usec)/1e6
}

func scaleRSS() int64 {
	data, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(data))
	if len(fields) < 2 {
		return 0
	}
	pages, _ := strconv.ParseInt(fields[1], 10, 64)
	return pages * int64(os.Getpagesize()) / 1024
}
