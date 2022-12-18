package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var port = flag.String("p", "", "运行端口")

func GetAllFiles(dirPth string) (files []string, err error) {
	dir, err := os.ReadDir(dirPth)
	if err != nil {
		return nil, err
	}

	PthSep := string(os.PathSeparator)
	// suffix = strings.ToUpper(suffix) //忽略后缀匹配的大小写

	for _, fi := range dir {
		// 过滤指定格式
		ok := strings.HasSuffix(fi.Name(), ".xml")
		if ok {
			files = append(files, dirPth+PthSep+fi.Name())
		}
	}

	return files, nil
}

// 目前为止，发现有两种格式：
// 1
// <www mdate="2012-01-04" key="homepages/28/10695">
// <author>...</author>
// <author>...</author>
// <title> ...</title>
// </www>
// 2
// 常规的
func searchName(name string, start, end int) (map[string]int, []Info) {
	m := make(map[string]int)
	name = "<author>" + name + "</author>"
	files, err := GetAllFiles("storage")
	if err != nil {
		fmt.Printf("获取xml文件失败，错误原因：%v", err)
		return nil, nil
	}

	var cacheInfoEntries []Info
	// 遍历每个chunk
	for _, f := range files {
		file, err := os.Open(f)
		if err != nil {
			fmt.Println("打开文件", file, "失败!")
			file.Close()
			continue
		}
		data := make([]byte, 1*(1<<20)*30) // 最大30MB
		n, err := file.Read(data)
		if err != nil {
			fmt.Println("读取文件", f, "失败!")
			file.Close()
			continue
		}
		file.Close()
		m[f] = 0

		// if start == 0 && end == 0 { // 不对时间设限制
		// 	m[f] = strings.Count(string(data[:n]), name)
		// } else {
		// 定位名字
		s := string(data[:n])
		var find bool // 在该chunk中，可曾找到过目标
		for {
			indexName := strings.Index(s, name)
			if indexName == -1 {
				if !find { // 加入一条没有数量的信息。这么做仅仅是为了client方便判断是否全部找到
					cacheInfoEntries = append(cacheInfoEntries, Info{
						Year:     -1,
						Number:   0,
						Chunk:    f,
						Location: -1,
					})
				}
				break
			}
			if !find {
				find = true
			}
			sReverse := s[:indexName] // 用于反向查找
			s = s[indexName:]
			// 定位<year>...</year>
			indexYear := strings.Index(s, "<year>")
			s = s[indexYear+6:]
			year, _ := strconv.Atoi(strings.Split(s, "<")[0])
			if year == 0 { // 通过这种方法没有正确找到year
				wwwIndex := strings.LastIndex(sReverse, "<www mdate=\"")
				s2 := sReverse[wwwIndex+12:]
				year, _ = strconv.Atoi(strings.Split(s2[:10], "-")[0])
				log.Println(year)
			}
			cacheInfoEntries = append(cacheInfoEntries, Info{
				Year:     year,
				Number:   1,
				Chunk:    f,
				Location: indexName,
			})
			if (start == 0 && end == 0) || (year >= start && year <= end) {
				m[f] += 1
			}
		}
		// }
	}
	fmt.Println(m)
	return m, cacheInfoEntries
}

func serve(conn net.Conn) {
	defer conn.Close()
	for {
		// 查dblp消息格式
		// flag=0;name;start;end

		// 查日志消息格式
		// flag=1;key

		data := make([]byte, 2048)
		n, err := conn.Read(data)
		if err != nil {
			if err == io.EOF {
				fmt.Println("客户端退出")
			} else {
				fmt.Println("从socket中读取信息失败：", err)
			}
			return
		}
		msg := string(data[:n]) // bytes -> string
		params := strings.Split(msg, ";")
		switch params[0] {
		case "0": // 查dblp
			fmt.Println("查dblp")
			// debug
			// fmt.Println(params)
			// fmt.Println(len(params))
			// for _, item := range params {
			// 	fmt.Println(item)
			// }
			author := params[1]
			start, _ := strconv.Atoi(params[2])
			end, _ := strconv.Atoi(params[3])
			fmt.Println(start, end)

			// 先查缓存
			exist, m := lookCaches(author, start, end)
			if !exist { // 缓存查询失败
				var cacheInfoEntries []Info
				m, cacheInfoEntries = searchName(author, start, end)
				appendCache(author, cacheInfoEntries)
			}
			// m := searchName(msg[2:])
			fmt.Println(len(m))
			res, _ := json.Marshal(m)
			conn.Write(res)
		case "1": // 查日志
			fmt.Println("查日志")
		default:
			fmt.Println("未知消息!")
		}
	}
}

func init() {
	file, err := os.OpenFile("./cache.json", os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		log.Printf("打开/创建cache.json失败：%v", err)
		return
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	err = decoder.Decode(&Caches)
	if err != nil {
		log.Println("反序列化cache.json失败", err)
		// return
	}
	for i, item := range Caches {
		Directory[item.Author] = i
	}
	fmt.Println("cache中的内容为：")
	fmt.Println(Caches)
}

func main() {
	// 定时持久化Caches
	go func() {
		term := -1
		for {
			log.Println("term is ", term)
			time.Sleep(time.Second * 10)
			saveCache(&term)
		}
	}()
	flag.Parse()
	if *port == "" {
		flag.Usage()
		return
	}
	listener, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Println("启动socket失败!")
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("与客户端建立socket失败!")
			return
		}
		go serve(conn)
	}
}
