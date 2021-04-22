// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package sync provides basic synchronization primitives such as mutual
// exclusion locks.  Other than the Once and WaitGroup types, most are intended
// for use by low-level library routines.  Higher-level synchronization is
// better done via channels and communication.
//
// Values containing the types defined in this package should not be copied.
package sync

import "sync/atomic"

// A Mutex is a mutual exclusion lock.
// Mutexes can be created as part of other structures;
// the zero value for a Mutex is an unlocked mutex.
type Mutex struct {
	state int32
	sema  uint32
}

// A Locker represents an object that can be locked and unlocked.
type Locker interface {
	Lock()
	Unlock()
}

const (
	mutexLocked = 1 << iota // mutex is locked
	mutexWoken
	mutexWaiterShift = iota
)

// Lock locks m.
// If the lock is already in use, the calling goroutine
// blocks until the mutex is available.
func (m *Mutex) Lock() {
	// Fast path: grab unlocked mutex.
	// 幸运 case，直接获取到锁
	if atomic.CompareAndSwapInt32(&m.state, 0, mutexLocked) {
		return
	}

	awoke := false
	for {
		old := m.state // 获取锁的状态，保存为 old
		new := old | mutexLocked // 新状态上锁
		if old&mutexLocked != 0 { // 锁的状态原本就是上锁了的状态
			new = old + 1<<mutexWaiterShift // 等待者的数量 +1
		}
		if awoke {
			// The goroutine has been woken from sleep,
			// so we need to reset the flag in either case.
			new &^= mutexWoken //去除唤醒标志
		}
		if atomic.CompareAndSwapInt32(&m.state, old, new) { // 尝试更新锁的状态
			if old&mutexLocked == 0 { //原本未上锁，又被更新成功了，说明我幸运地获取到了锁，可以愉快滴返回了
				break
			}
			runtime_Semacquire(&m.sema) //原本就是上锁的状态，那我只能阻塞等待了(通过获取信号量的方式阻塞等待)
			awoke = true // 上一步的阻塞等待结束了，说明我是被唤醒的，做唤醒标记
			//被唤醒以后还是要去抢夺锁，而不是直接得到锁，这就给了新来的 goroutine 一些获取锁的机会
		}
	}
}

// Unlock unlocks m.
// It is a run-time error if m is not locked on entry to Unlock.
//
// A locked Mutex is not associated with a particular goroutine.
// It is allowed for one goroutine to lock a Mutex and then
// arrange for another goroutine to unlock it.
func (m *Mutex) Unlock() {
	// Fast path: drop lock bit.
	// 去除锁的标识位，这一步执行结束，如果有其它的 goroutine 来抢夺锁，是可以成功获取到锁的
	new := atomic.AddInt32(&m.state, -mutexLocked)
	// 解锁一个没有上锁的锁，直接panic
	if (new+mutexLocked)&mutexLocked == 0 {
		panic("sync: unlock of unlocked mutex")
	}

	old := new
	for {
		// If there are no waiters or a goroutine has already
		// been woken or grabbed the lock, no need to wake anyone.
		// 没有其它 waiter，或是已经有其它 goroutine 获取到锁，或是有其它waiter被唤醒
		// 这里要说一下，为什么会有被唤醒的 waiter，因为上一步的解锁操作完成后，如果有新来的 goroutine 获取到锁，并执行结束，同时完成了解锁操作，它就有可能唤醒了其它 waiter
		if old>>mutexWaiterShift == 0 || old&(mutexLocked|mutexWoken) != 0 {
			return
		}
		// Grab the right to wake someone.
		// 尝试去唤醒一个 waiter
		// 为什么说是尝试？因为在尝试的过程中，Mutex 的状态可能已经被其它 goroutine 改变了
		new = (old - 1<<mutexWaiterShift) | mutexWoken // 减去一个 waiter 数量，然后做 |mutexWoken 操作，将唤醒标识位置为1
		if atomic.CompareAndSwapInt32(&m.state, old, new) {	// 尝试去做这个唤醒操作，更新成功才能有资格进行唤醒操作
			runtime_Semrelease(&m.sema) // 唤醒1个 waiter
			return // 老子的解锁操作终于做完了
		}
		// 完了，上一步所说的唤醒操作没成功！没办法只好获取最新的锁状态，再重复一次
		old = m.state
	}
}
