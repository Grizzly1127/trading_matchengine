package skiplist

import "math/rand/v2"

const maxSkiplistLevel = 16

type SkipList struct {
	level   int
	length  int
	compare func(a, b any) int // 回调函数
	head    *SkipNode
	rand    *rand.Rand
}

type SkipNode struct {
	value   any
	forward []*SkipNode
}

type Iterator struct {
	current *SkipNode
}

func NewSkipList(compare func(a, b any) int) *SkipList {
	return &SkipList{
		level:   1,
		length:  0,
		compare: compare,
		head:    &SkipNode{forward: make([]*SkipNode, maxSkiplistLevel)},
		rand:    rand.New(rand.NewPCG(1, 2)),
	}
}

func (sl *SkipList) RandomLevel() int {
	level := 1

	for sl.rand.Float64() < 0.25 && level < maxSkiplistLevel {
		level++
	}
	return level
}

// Insert 插入新节点
func (sl *SkipList) Insert(value any) {
	update := make([]*SkipNode, sl.level)
	current := sl.head

	// 从最高层开始向下查找插入位置
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && sl.compare(current.forward[i].value, value) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	// 到达底层，检查key是否已存在，存在则更新
	current = current.forward[0]
	if current != nil && sl.compare(current.value, value) == 0 {
		current.value = value
		return
	}

	// 随机生成新节点的层数
	newLevel := sl.RandomLevel()
	if newLevel > sl.level {
		for range newLevel - sl.level {
			update = append(update, sl.head)
		}
		// 扩展head的层数
		for len(sl.head.forward) < newLevel {
			sl.head.forward = append(sl.head.forward, nil)
		}
		sl.level = newLevel
	}

	// 创建新节点
	newNode := &SkipNode{
		value:   value,
		forward: make([]*SkipNode, newLevel),
	}

	// 更新forward指针
	for i := range newLevel {
		newNode.forward[i] = update[i].forward[i]
		update[i].forward[i] = newNode
	}

	sl.length++
}

// Search 查找节点
func (sl *SkipList) Search(value any) (any, bool) {
	current := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && sl.compare(current.forward[i].value, value) < 0 {
			current = current.forward[i]
		}
	}
	current = current.forward[0]
	if current != nil && sl.compare(current.value, value) == 0 {
		return current.value, true
	}
	return nil, false
}

// Delete 删除节点
func (sl *SkipList) Delete(value any) bool {
	update := make([]*SkipNode, sl.level)
	current := sl.head

	// 查找要删除的节点
	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && sl.compare(current.forward[i].value, value) < 0 {
			current = current.forward[i]
		}
		update[i] = current
	}

	// 到达底层，检查key是否存在
	current = current.forward[0]
	if current == nil || sl.compare(current.value, value) != 0 {
		return false // 节点不存在
	}

	// 删除节点：在所有指向 current 的层上断开链接
	for i := range sl.level {
		if update[i].forward[i] == current {
			update[i].forward[i] = current.forward[i]
		}
	}

	// 更新level和length
	for sl.level > 1 && sl.head.forward[sl.level-1] == nil {
		sl.level--
	}
	sl.length--
	return true
}

// contains 检查是否包含指定key
func (sl *SkipList) Contains(key any) bool {
	_, ok := sl.Search(key)
	return ok
}

// Size 返回跳表长度
func (sl *SkipList) Size() int {
	return sl.length
}

// IsEmpty 检查跳表是否为空
func (sl *SkipList) IsEmpty() bool {
	return sl.length == 0
}

// Clear 清空跳表
func (sl *SkipList) Clear() {
	sl.head = &SkipNode{forward: make([]*SkipNode, maxSkiplistLevel)}
	sl.level = 1
	sl.length = 0
}

// Front 返回最小 key 的节点（升序跳表表头）。
func (sl *SkipList) Front() (value any, ok bool) {
	node := sl.head.forward[0]
	if node == nil {
		return nil, false
	}
	return node.value, true
}

// Iterator 返回迭代器
func (sl *SkipList) Iterator() *Iterator {
	return &Iterator{
		current: sl.head.forward[0],
	}
}

// HasNext 检查是否还有下一个节点
func (itr *Iterator) HasNext() bool {
	return itr.current != nil
}

// Next 移动到下一个节点
func (itr *Iterator) Next() (any, bool) {
	if itr.current == nil {
		return nil, false
	}
	value := itr.current.value
	itr.current = itr.current.forward[0]
	return value, true
}
