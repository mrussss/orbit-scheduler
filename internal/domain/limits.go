package domain

const (
	// MaxResultBytes is the largest canonical task result accepted by the
	// worker reporting contract.
	MaxResultBytes = 1 << 20
	// MaxGRPCMessageBytes leaves envelope headroom around a maximum-size task
	// payload or result while application-level limits remain authoritative.
	MaxGRPCMessageBytes = 2 << 20
)
