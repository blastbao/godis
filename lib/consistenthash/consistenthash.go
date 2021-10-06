package consistenthash

import (
	"hash/crc32"
	"sort"
	"strconv"
	"strings"
)

// HashFunc defines function to generate hash code
type HashFunc func(data []byte) uint32

// Map stores nodes and you can pick node from Map
type Map struct {
	hashFunc HashFunc
	replicas int
	keys     []int // sorted
	hashMap  map[int]string
}

// New creates a new Map
func New(replicas int, fn HashFunc) *Map {
	m := &Map{
		replicas: replicas,	// 每个物理节点会产生 replicas 个虚拟节点
		hashFunc: fn,
		hashMap:  make(map[int]string),// 虚拟节点 hash 值到物理节点地址的映射
	}
	if m.hashFunc == nil {
		m.hashFunc = crc32.ChecksumIEEE
	}
	return m
}

// IsEmpty returns if there is no node in Map
func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

// AddNode add the given nodes into consistent hash circle
//
func (m *Map) AddNode(keys ...string) {
	for _, key := range keys {
		if key == "" {
			continue
		}
		for i := 0; i < m.replicas; i++ {
			// 使用 i + key 作为一个虚拟节点，计算虚拟节点的 hash 值
			hash := int(m.hashFunc([]byte(strconv.Itoa(i) + key)))
			// 将虚拟节点添加到环上
			m.keys = append(m.keys, hash)
			// 注册虚拟节点到物理节点的映射
			m.hashMap[hash] = key
		}
	}
	sort.Ints(m.keys)
}

// support hash tag
//
// 提取 key 中由 "{}" 扩起来的部分，作为 Partition Key 。
func getPartitionKey(key string) string {
	beg := strings.Index(key, "{")
	if beg == -1 {
		return key
	}
	end := strings.Index(key, "}")
	if end == -1 || end == beg+1 {
		return key
	}
	return key[beg+1 : end]
}

// PickNode gets the closest item in the hash to the provided key.
func (m *Map) PickNode(key string) string {
	if m.IsEmpty() {
		return ""
	}

	// 提取分区 key
	partitionKey := getPartitionKey(key)

	// 计算 Hash 值
	hash := int(m.hashFunc([]byte(partitionKey)))

	// Binary search for appropriate replica.
	// 二分查找，找到哈希环上 key 归属的区间
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	// Means we have cycled back to the first replica.
	// 没有找到，重置为 0
	if idx == len(m.keys) {
		idx = 0
	}

	// 返回区间
	return m.hashMap[m.keys[idx]]
}
