package worker

// Expose the delivery boundary only to integration tests so they can interrupt
// the real consumer between a broker Confirm and the original ACK.
var RunFollowingFanoutWithQuarantineForTest = runFollowingFanoutConsumer
