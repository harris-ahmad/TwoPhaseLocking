package txn

import (
	"fmt"
	"sync"
	"sync/atomic"
	"github.com/harris-ahmad/TwoPhaseLocking/lock"
)

type TxnState int32

const (
	Active TxnState = iota
	Committed
	Aborted
)

func (s TxnState) String() string{
	switch s {
	case Active:
		return "Active"
	case Committed:
		return "Committed"
	case Aborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

type Phase int32

const (
	Growing Phase = iota
	Shrinking
)

func (p Phase) String() string{
	switch p {
	case Growing:
		return "Growing"
	case Shrinking:
		return "Shrinking"
	default:
		return "Unknown"
	}
}