package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
)

type Cache struct {
	Author string `json:"author"`
	From   int    `json:"from"`
	To     int    `json:"to"`
	Infos  []Info `json:"infos"`
}

type Info struct {
	Year     int    `json:"year"`
	Number   int    `json:"number"`
	Chunk    string `json:"chunk"`    // 表示该信息来自哪个chunk
	Location int    `json:"location"` // 表示该信息来自该chunk的哪个位置，用于唯一标识一个记录
}

var Caches []Cache
var Directory = make(map[string]int) // 用于快速查找Cache
var Term int                         // 用于判断Caches是否有更新
var CachesMutex sync.Mutex

// bool标识缓存是否存在，map标识结果
// 只有bool为true，map才有意义
func lookCaches(author string, start, end int) (bool, map[string]int) {
	CachesMutex.Lock()
	defer CachesMutex.Unlock()
	index, ok := Directory[author]
	if !ok { // 说明缓存中没有记录该作者的信息
		return false, nil
	}
	cache := Caches[index] // 获取缓存

	fmt.Println(cache)
	var all bool
	m := make(map[string]int)
	if start == 0 && end == 0 {
		all = true
	}
	for _, item := range cache.Infos {
		if all || (item.Year >= start && item.Year <= end) {
			m[item.Chunk] += 1
		}
	}
	return true, m
}

// 更新Caches
func appendCache(author string, infos []Info) {
	CachesMutex.Lock()
	defer CachesMutex.Unlock()
	start := infos[0].Year
	end := infos[0].Year
	for _, item := range infos {
		if item.Year < start {
			start = item.Year
		}
		if item.Year > end {
			end = item.Year
		}
	}
	if _, ok := Directory[author]; !ok {
		Term += 1
		Caches = append(Caches, Cache{
			Author: author,
			From:   start,
			To:     end,
			Infos:  infos,
		})
		Directory[author] = len(Caches) - 1
	}
	log.Println(Caches)
}

// 持久化cache
func saveCache() {
	CachesMutex.Lock()
	defer CachesMutex.Unlock()
	log.Println("持久化cache")
	file, err := os.OpenFile("./cache.json", os.O_WRONLY|os.O_TRUNC, 0755)
	if err != nil {
		log.Println("打开cache.json失败：", err)
		return
	}
	defer file.Close()
	data, _ := json.Marshal(Caches)
	file.Write(data)
}
