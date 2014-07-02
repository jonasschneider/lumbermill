/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Changelog:

 - 2014-07-02 (apg): Modified to support storing a ChanGroup instead
   of string key

*/

package main

import (
	"hash/crc32"
	"sort"
	"strconv"
)

type HashFn func(data []byte) uint32

type HashRing struct {
	hash     HashFn
	replicas int
	keys     []int // Sorted
	hashMap  map[int]*ChanGroup
}

func NewHashRing(replicas int, fn HashFn) *HashRing {
	m := &HashRing{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[int]*ChanGroup),
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
	}
	return m
}

// Returns true if there are no items available.
func (m *HashRing) IsEmpty() bool {
	return len(m.keys) == 0
}

// Adds some keys to the hash.
func (m *HashRing) Add(groups ...*ChanGroup) {
	for _, group := range groups {
		for i := 0; i < m.replicas; i++ {
			hash := int(m.hash([]byte(strconv.Itoa(i) + group.Name)))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = group
		}
		sort.Ints(m.keys)
	}
}

// Gets the closest item in the hash to the provided key.
func (m *HashRing) Get(key string) *ChanGroup {
	if m.IsEmpty() {
		return nil
	}

	hash := int(m.hash([]byte(key)))

	// Linear search for appropriate replica.
	for _, v := range m.keys {
		if v >= hash {
			return m.hashMap[v]
		}
	}

	// Means we have cycled back to the first replica.
	return m.hashMap[m.keys[0]]
}
