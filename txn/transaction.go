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

type Storage interface{
	Read(key string) ([]byte, error)
	Write(key string, value []byte) error
}

type Transaction struct{
	ID uint64
	state TxnState
	phase Phase
	
	lockMgr *lock.Manager
	storage Storage
	mu sync.Mutex
}

type Manager struct {
	lockMgr *lock.Manager
	storage Storage
	nextTxnID atomic.Uint64
}

func NewManager(storage Storage) *Manager{
	return &Manager{
		lockMgr: lock.NewManager(),
		storage: storage,
	}
}

func (tm *Manager) Begin() *Transaction{
	txnID := tm.nextTxnID.Add(1)

	return &Transaction{
		ID: txnID,
		state: Active,
		phase: Growing,
		lockMgr: tm.lockMgr,
		storage: tm.storage,
	}
}

func (t *Transaction) Read(key string) ([]byte, error){
	t.mu.Lock()
	if t.state != Active {
		t.mu.Unlock()
		return nil, fmt.Errorf("transaction %d is not active (state: %s)", t.ID, t.state.String())
	}
	if t.phase != Growing{
		t.mu.Unlock()
		return nil, fmt.Errorf("transaction %d is not in growing phase (phase: %s)", t.ID, t.phase.String())
	}
	t.mu.Unlock()
	req,err := t.lockMgr.AcquireLock(t.ID,key,lock.Shared)
	if err != nil{
		return nil, err
	}
	req.Wait()
	return t.storage.Read(key)
}

func (t *Transaction) Write(key string, value []byte) error{
	t.mu.Lock()
	if t.state != Active{
		t.mu.Unlock()
		return fmt.Errorf("transaction %d is not active (state: %s)", t.ID, t.state.String())
	}
	if t.phase != Growing{
		t.mu.Unlock()
		return fmt.Errorf("transaction %d is not in growing phase (phase: %s)", t.ID, t.phase.String())
	}
	t.mu.Unlock()
	req,err := t.lockMgr.AcquireLock(t.ID,key,lock.Exclusive)
	if err != nil{
		return err
	}
	req.Wait()
	return t.storage.Write(key,value)
}

func (t *Transaction) Commit() error{
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.state != Active{
		return fmt.Errorf("transaction %d is not active (state: %s)", t.ID, t.state.String())
	}
	t.phase=Shrinking
	t.state=Committed
	t.lockMgr.ReleaseAll(t.ID)

	return nil
}

func (t *Transaction) Abort() error{
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.state != Active{
		return fmt.Errorf("transaction %d is not active (state: %s)", t.ID, t.state.String())
	}
	t.phase=Shrinking
	t.state=Aborted
	t.lockMgr.ReleaseAll(t.ID)

	return nil
}

func (t* Transaction) GetState() TxnState{
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.state
}

func (t *Transaction) GetPhase() Phase{
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.phase
}
func (t *Transaction) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return fmt.Sprintf("Txn(ID=%d, State=%s, Phase=%s)", t.ID, t.state.String(), t.phase.String())
}