package lock

import (
	"fmt"
	"sync"
)

type LockMode int

const (
	Shared LockMode = iota
	Exclusive
)

type LockRequest struct {
	TxId uint64
	Resource string
	Mode LockMode
	Granted bool 
	done chan struct{}
}

type resourceLocks struct{
	holders map[uint64]*LockRequest
	waitQueue []*LockRequest
}

type Manager struct{
	mu sync.Mutex
	resources map[string]*resourceLocks
	txnLocks map[uint64]map[string]*LockRequest
}

func (m LockMode) String() string {
	switch m {
	case Shared:
		return "Shared"
	case Exclusive:
		return "Exclusive"
	default:
		return "Unknown"
	}
}

func isCompatible(held, requested LockMode) bool {
	return held==Shared && requested==Shared
}

func (req *LockRequest) Wait(){
	<-req.done
}

func NewManager() *Manager{
	return &Manager{
		resources: make(map[string]*resourceLocks),
		txnLocks: make(map[uint64]map[string]*LockRequest),
	}
}

func (lm *Manager) AcquireLock(txId uint64, resource string, mode LockMode) (*LockRequest, error){
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if lm.resources[resource]==nil{
		lm.resources[resource]=&resourceLocks{
			holders: make(map[uint64]*LockRequest),
			waitQueue: make([]*LockRequest, 0),
		}
	}

	rl := lm.resources[resource]

	//check if a txn already holds a lock on this resource
	if existing, ok:= rl.holders[txId]; ok{
		if existing.Mode==Shared && mode==Exclusive{
			return lm.upgradeLock(txId, resource, rl)
		}
		return existing, nil 
	}

	req := &LockRequest{
		TxId: txId,
		Resource: resource,
		Mode: mode,
		Granted: false,
		done: make(chan struct{}),
	}

	compatible:= lm.isCompatibleWithHolders(rl, mode)

	if compatible && len(rl.waitQueue)==0 {
		lm.grantLock(txId,req,rl)
	} else{
		rl.waitQueue = append(rl.waitQueue, req)
	}

	return req,nil
}

func (lm *Manager) isCompatibleWithHolders(rl *resourceLocks, mode LockMode) bool{
	if len(rl.holders)==0{
		return true 
	}
	for _,holder := range rl.holders{
		if !isCompatible(holder.Mode,mode){
			return false
		}
	}
	return true
}

func (lm *Manager) grantLock(txId uint64, req *LockRequest, rl *resourceLocks){
	req.Granted=true
	rl.holders[txId]=req
	close(req.done)
	if lm.txnLocks[txId]==nil{
		lm.txnLocks[txId]=make(map[string]*LockRequest)
	}
	lm.txnLocks[txId][req.Resource]=req
}

func (lm *Manager) upgradeLock(txId uint64, resource string, rl *resourceLocks) (*LockRequest, error){
	//if there is only one holder, upgrade
	if len(rl.holders)==1{
		existing := rl.holders[txId]
		existing.Mode=Exclusive
		return existing,nil
	}
	req := &LockRequest{
		TxId: txId,
		Resource: resource,
		Mode: Exclusive,
		Granted: false,
		done: make(chan struct{}),
	}
	rl.waitQueue=append([]*LockRequest{req}, rl.waitQueue...)
	delete(rl.holders,txId)
	delete(lm.txnLocks[txId], resource)
	return req, nil
}

func (lm *Manager) ReleaseLock(txId uint64, resource string) error{
	lm.mu.Lock()
	defer lm.mu.Unlock()

	rl := lm.resources[resource]
	if rl==nil{
		return fmt.Errorf("no locks on resource %s", resource)
	}
	if _,ok := rl.holders[txId]; !ok {
		return fmt.Errorf("txn %d does not hold lock on %s", txId, resource)
	}
	delete(rl.holders, txId)
	delete(lm.txnLocks[txId], resource)

	lm.tryGrantWaiting(rl)
	return nil
}

func (lm *Manager) ReleaseAll(txId uint64){
	lm.mu.Lock()
	defer lm.mu.Unlock()

	locks:= lm.txnLocks[txId]
	if locks==nil {
		return
	}
	for resource := range locks{
		rl := lm.resources[resource]
		if rl != nil {
			delete(rl.holders,txId)
			lm.tryGrantWaiting(rl)
		}
	}
	delete(lm.txnLocks, txId)
}

// grant locks in FIFO order for max fairness
func (lm *Manager) tryGrantWaiting(rl *resourceLocks){
	//no waiting requests so simply return
	if len(rl.waitQueue)==0{
		return
	}
	granted :=0
	for i:=0; i<len(rl.waitQueue); i++{
		req:= rl.waitQueue[i]
		if lm.isCompatibleWithHolders(rl,req.Mode){
			lm.grantLock(req.TxId,req,rl)
			granted++
			// if exclusive lock granted, stop processing further
			if req.Mode==Exclusive{
				break
			}
		} else {
			break
		}
	}
	if granted>0{
		rl.waitQueue=rl.waitQueue[granted:]
	}
}