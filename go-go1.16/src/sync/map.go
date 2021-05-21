// Copyright 2016 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package sync

import (
	"sync/atomic"
	"unsafe"
)

type Map struct {
	mu     Mutex
	read   atomic.Value // readOnly
	dirty  map[interface{}]*entry
	misses int
}

type readOnly struct {
	m       map[interface{}]*entry
	amended bool
}

// 标记一个 entry 被删除了
var expunged = unsafe.Pointer(new(interface{}))

type entry struct {
	p unsafe.Pointer
}

func newEntry(i interface{}) *entry {
	return &entry{p: unsafe.Pointer(&i)}
}

func (m *Map) Load(key interface{}) (value interface{}, ok bool) {
	read, _ := m.read.Load().(readOnly)
	e, ok := read.m[key]
	if !ok && read.amended { // read 里没有，并且 dirty 中包含 read 不存在的元素，去 dirty 试试看
		m.mu.Lock() // 锁住 dirty
		// 二次检查，万一在抢夺锁的过程中，read 被更新了呢，再去 read 尝试一次
		// dirty 已经被锁了，如果这次 read 还没有，那锁释放前，都不可能再有了
		// 因为 read 若想新增 key，只能通过把 dirty 升级为 read 完成，而 dirty 的升级需要持有锁
		read, _ = m.read.Load().(readOnly)
		e, ok = read.m[key]
		if !ok && read.amended { // read 还是没有，并且处于修正状态
			e, ok = m.dirty[key] // 此时不管dirty中是否存在，miss 数都会 +1
			m.missLocked()       // miss 处理，miss 次数多的话，考虑把 dirty 升级成为 read
		}
		m.mu.Unlock()
	}
	if !ok { // 最终还是没找到，返回
		return nil, false
	}
	// 找到了 key 对应的 entry，但是 entry 也有可能是被标记为删除的
	return e.load()
}

func (e *entry) load() (value interface{}, ok bool) {
	p := atomic.LoadPointer(&e.p)
	// p == expunged 和 p == nil 都是在什么场景下出现的？参照文章中的例子
	// 已经被删除了，直接返回
	if p == nil || p == expunged {
		return nil, false
	}
	return *(*interface{})(p), true
}

// 存储一个key，会出现哪些情况？
func (m *Map) Store(key, value interface{}) {
	read, _ := m.read.Load().(readOnly)
	// 先去 read 查找一下，是否存在 key 对应的节点，存在的话尝试直接更新
	if e, ok := read.m[key]; ok && e.tryStore(&value) { // 节点存在，还是一个未标记清除的节点，直接存储成功可以返回了
		return
	}

	// 试图在 read 里更新的操作没有执行成功，那需要在 dirty 里进行了
	m.mu.Lock()
	read, _ = m.read.Load().(readOnly) // 二次检查 read 中是否存在 key 对应的节点，因为在尝试锁的过程中，read 可能已经更新了
	if e, ok := read.m[key]; ok {      // read 中存在要更新的 key
		if e.unexpungeLocked() {
			// key 对应的节点已被标记 expunged，等着被删除，e.unexpungeLocked() 在返回 true 的同时，也清除了 expunged 标记
			// 对应的场景是:
			// read  -> {1: entry{p: expunged}}, {2: entry{p: p2}}, {3: entry{p: p3}}
			// dirty -> {2: entry{p: p2}}, {3: entry{p: p3}}
			// 需要把这个节点加到 dirty 里面，否则下次的升级操作会导致这个 key=1 丢失
			m.dirty[key] = e
		}
		// entry 存入新的正确的 value
		// read 和 dirty 中的 entry 是同一个，都是持有了 entry 的指针
		e.storeLocked(&value)
	} else if e, ok := m.dirty[key]; ok {
		// read 中不存在，dirty 中存在 key 的映射
		// 直接更新 entry 保存的 value
		e.storeLocked(&value)
	} else {               // read 和 dirty 都不存在，新增
		if !read.amended { // 要加入新的 key，如果 read 是完整的，那要把它标记为不完整，因为我们要在 dirty 中加入一个新的映射关系
			m.dirtyLocked() // 如果 dirty 是空的，会先拷贝一份 read 给 dirty。read 是完整的才会出现这种情况，read 如果已经不完整了，那 dirty 肯定不是 nil
			m.read.Store(readOnly{m: read.m, amended: true})
		}
		m.dirty[key] = newEntry(value) // dirty 加入新的映射
	}
	m.mu.Unlock()
}

// 尝试存储 value 到 entry 节点，如果节点被标记为已删除，则返回失败
func (e *entry) tryStore(i *interface{}) bool {
	for {
		p := atomic.LoadPointer(&e.p)
		// entry 被标记为清除了，那就不能在这个 entry 里做任何操作了
		if p == expunged {
			return false
		}
		// CAS 操作尝试更新
		if atomic.CompareAndSwapPointer(&e.p, p, unsafe.Pointer(i)) {
			return true
		}
	}
}

// 去除 expunged 标记
// 如果节点之前被标记了 expunged，清除掉，并返回 true
// 否则返回 false
func (e *entry) unexpungeLocked() (wasExpunged bool) {
	return atomic.CompareAndSwapPointer(&e.p, expunged, nil)
}

// 存储一个 value 到 entry 节点
func (e *entry) storeLocked(i *interface{}) {
	atomic.StorePointer(&e.p, unsafe.Pointer(i))
}

// key 已经存在，就加载对应的 value
// key 不存在，就新增 key-value 映射
func (m *Map) LoadOrStore(key, value interface{}) (actual interface{}, loaded bool) {
	read, _ := m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok {
		actual, loaded, ok := e.tryLoadOrStore(value)
		if ok {
			return actual, loaded
		}
	}
	m.mu.Lock()
	read, _ = m.read.Load().(readOnly)
	if e, ok := read.m[key]; ok {
		if e.unexpungeLocked() {
			m.dirty[key] = e
		}
		actual, loaded, _ = e.tryLoadOrStore(value)
	} else if e, ok := m.dirty[key]; ok {
		actual, loaded, _ = e.tryLoadOrStore(value)
		m.missLocked()
	} else {
		if !read.amended {
			m.dirtyLocked()
			m.read.Store(readOnly{m: read.m, amended: true})
		}
		m.dirty[key] = newEntry(value)
		actual, loaded = value, false
	}
	m.mu.Unlock()

	return actual, loaded
}

// 原子操作
// 如果 entry 被标记为已清除，直接返回 ok = false
// 如果 entry 已经保存了其它 value，返回 actual=value, loaded=true, ok=true
// 存储 value 到 entry
func (e *entry) tryLoadOrStore(i interface{}) (actual interface{}, loaded, ok bool) {
	p := atomic.LoadPointer(&e.p)
	if p == expunged {
		return nil, false, false
	}
	if p != nil {
		return *(*interface{})(p), true, true
	}
	ic := i
	for {
		if atomic.CompareAndSwapPointer(&e.p, nil, unsafe.Pointer(&ic)) { // 存储 value 到 entry
			return i, false, true
		}
		p = atomic.LoadPointer(&e.p)
		if p == expunged {
			return nil, false, false
		}
		if p != nil {
			return *(*interface{})(p), true, true
		}
	}
}

// 删除操作，只是把节点的 p 改为 nil，并没有真正删除
// 如果 read 包含要删除的 key，把 key 对应的 entry 的 p 更新为 nil
// 如果 dirty 包含要删除的 key，把 key 从 dirty 中删除，把 key 对应的 entry 的 p 更新为 nil
func (m *Map) LoadAndDelete(key interface{}) (value interface{}, loaded bool) {
	read, _ := m.read.Load().(readOnly)
	e, ok := read.m[key]
	if !ok && read.amended {
		m.mu.Lock()
		read, _ = m.read.Load().(readOnly)
		e, ok = read.m[key]
		if !ok && read.amended {
			e, ok = m.dirty[key]
			delete(m.dirty, key)
			m.missLocked()
		}
		m.mu.Unlock()
	}
	if ok {
		return e.delete()
	}
	return nil, false
}

// 删除一个 key
// 并不是真的删除，只是把 key 对以的 entry 存储的 p 更新为 nil
func (m *Map) Delete(key interface{}) {
	m.LoadAndDelete(key)
}

// 删除一个 entry，其实是把 entry 的 p 修改为 nil
func (e *entry) delete() (value interface{}, ok bool) {
	for {
		p := atomic.LoadPointer(&e.p)
		if p == nil || p == expunged {
			return nil, false
		}
		if atomic.CompareAndSwapPointer(&e.p, p, nil) {
			return *(*interface{})(p), true
		}
	}
}

// 遍历
// amended 为 true 时，升级 dirty 为 read
func (m *Map) Range(f func(key, value interface{}) bool) {
	read, _ := m.read.Load().(readOnly)
	if read.amended {
		m.mu.Lock()
		read, _ = m.read.Load().(readOnly)
		if read.amended {
			read = readOnly{m: m.dirty}
			m.read.Store(read)
			m.dirty = nil
			m.misses = 0
		}
		m.mu.Unlock()
	}

	for k, e := range read.m {
		v, ok := e.load()
		if !ok {
			continue
		}
		if !f(k, v) {
			break
		}
	}
}

// misses 处理
// misses 次数到了，升级 dirty 为 read
// 不用考虑并发读写问题，missLocked 调用的地方都先获取了锁
func (m *Map) missLocked() {
	m.misses++
	if m.misses < len(m.dirty) {
		return
	}
	// miss 次数大于等于 dirty 长度时，把 dirty 升级为 read，并清空 dirty
	m.read.Store(readOnly{m: m.dirty})
	m.dirty = nil // 清空 dirty
	m.misses = 0
}

// 把 read 拷贝一份给 dirty
// 拷贝过程中会检查 entry，如果 entry 的 p 为 nil，说明它被删除了
// 被删除的 entry 不用拷贝，不过会把 entry 的 p 更新为 expunged
func (m *Map) dirtyLocked() {
	if m.dirty != nil {
		return
	}
	read, _ := m.read.Load().(readOnly)
	m.dirty = make(map[interface{}]*entry, len(read.m))
	for k, e := range read.m {
		if !e.tryExpungeLocked() { // 没有被删除，拷贝到 dirty
			m.dirty[k] = e
		}
	}
}

// entry 的 p 为 nil，添加 expunged 标记，返回 true
// entry 的 p 不为 nil，返回 false
func (e *entry) tryExpungeLocked() (isExpunged bool) {
	p := atomic.LoadPointer(&e.p)
	for p == nil {
		if atomic.CompareAndSwapPointer(&e.p, nil, expunged) {
			return true
		}
		p = atomic.LoadPointer(&e.p)
	}
	return p == expunged
}
