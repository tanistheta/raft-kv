package maelstrom

// Maelstrom's standard error codes
// (https://github.com/jepsen-io/maelstrom/blob/main/doc/protocol.md#errors).
// Only the ones this package returns.
const (
	errTemporarilyUnavailable = 11 // not leader, or leader not fresh yet - client should retry
	errMalformedRequest       = 12 // request shape this server doesn't support (e.g. value contains a space)
	errAbort                  = 14 // a pending write was overwritten before it committed
	errKeyDoesNotExist        = 20
	errPreconditionFailed     = 22 // cas: current value didn't match "from"
)