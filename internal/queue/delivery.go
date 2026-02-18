package queue

// Delivery is a transport-agnostic message envelope.
type Delivery struct {
	Body []byte
	Ack  func() error
	Nack func(toDLQ bool) error
}
