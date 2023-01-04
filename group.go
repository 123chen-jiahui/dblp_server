package main

import (
	"log"
	"net"
	"strings"
	"sync"
	"time"
)

// MemberListEntry 组成员表项
type MemberListEntry struct {
	Addr string // 组成员地址
	Ip   string
	Port string
	Time int64
}

// Message 消息格式
type Message struct {
	Command string `json:"command"` // join、leave、fail...
	// Addr    string
	// Ip      string `json:"ip"`
	// Port    string `json:"port"`
}

// MemberList 组成员列表
// var MemberList []MemberListEntry
var MemberList []string
var mu sync.Mutex

var HeartBeatChannel chan bool
var JoinChannel chan bool

// UDPListener UDP句柄
var UDPListener *net.UDPConn

// 创建UDP监听句柄
func createUDPHandler() {
	var err error
	UDPListener, err = net.ListenUDP("udp", UdpAddr)
	if err != nil {
		log.Println("UDP监听出错：", err)
		return
	}
}

// 是否是引荐人
var isReferrer bool
var UdpAddr *net.UDPAddr // 本进程标识
var RefAddr *net.UDPAddr // 引荐人标识

func MemberListTobytes() []byte {
	mu.Lock()
	defer mu.Unlock()
	addrs := strings.Join(MemberList, "\n")
	return []byte(addrs)
}

// 获取下一个节点的地址，使用该函数前要获取锁
func findNextAddr() *net.UDPAddr {
	var des *net.UDPAddr
	for i, v := range MemberList {
		if v == UdpAddr.String() {
			next := MemberList[(i+1)%len(MemberList)]
			des, _ = net.ResolveUDPAddr("udp", next)
			break
		}
	}
	return des
}

// 转发信息
func transmit(data []byte) {
	mu.Lock()
	des := findNextAddr()
	mu.Unlock()
	sendMessage(data, des)
}

// 向引荐人发送加入的请求
// 因为一系列原因，接收方可能无法接收到信号，所以如果失败则一直尝试发送
func join() {
	for {
	Loop:
		data := []byte("join " + UdpAddr.String())

		sendMessage(data, RefAddr)
		t := time.Now()
		for {
			time.Sleep(time.Millisecond * 100)
			select {
			case <-JoinChannel:
				return
			default:
				if time.Since(t).Milliseconds() > 500 {
					goto Loop
				}
			}
		}
	}
}

func add() {
	mu.Lock()
	msg := "addd " + strings.Join(MemberList, " ")
	data := []byte(msg)
	var des *net.UDPAddr
	for i, v := range MemberList {
		if v == UdpAddr.String() {
			next := MemberList[(i+1)%len(MemberList)]
			des, _ = net.ResolveUDPAddr("udp", next)
			break
		}
	}
	mu.Unlock()
	sendMessage(data, des)
}

// 节点主动退出
func leave() {
	// 消息格式: left 谁寄了(自己) 所有节点
	// 默认发送给下一个节点，由下一个节点负责转发
	mu.Lock()
	var members []string
	for _, v := range MemberList {
		if v == UdpAddr.String() {
			continue
		}

		members = append(members, v)
	}
	msg := "left " + UdpAddr.String() + " " + strings.Join(members, " ")
	data := []byte(msg)
	des := findNextAddr()
	MemberList = []string{UdpAddr.String()}
	// close(HeartBeatChannel)
	// close(JoinChannel)
	mu.Unlock()
	sendMessage(data, des)
}

// 对leave的扩充，会进一步转发消息，处理逻辑类似fail
func realLeave(leaveName string) {
	// 消息格式: leav 初始是谁发的(避免一直循环) 谁寄了 所有节点
	mu.Lock()
	msg := "leav " + UdpAddr.String() + " " + leaveName + " " + strings.Join(MemberList, " ")
	data := []byte(msg)
	des := findNextAddr()
	mu.Unlock()
	sendMessage(data, des)
}

func fail(failName string) {
	// 消息格式: fail 初始是谁发的(避免一直循环) 谁寄了 所有节点
	mu.Lock()
	msg := "fail " + UdpAddr.String() + " " + failName + " " + strings.Join(MemberList, " ")
	data := []byte(msg)
	des := findNextAddr()
	mu.Unlock()
	sendMessage(data, des)
}

func heartBeat() {
	for {
	Loop:
		time.Sleep(time.Millisecond * 100)
		mu.Lock()
		if len(MemberList) <= 1 {
			mu.Unlock()
			continue
		}
		msg := "ping " + UdpAddr.String()
		mu.Unlock()
		data := []byte(msg)
		// sendMessage(data)
		var des *net.UDPAddr
		mu.Lock()
		for i, v := range MemberList {
			if v == UdpAddr.String() {
				next := MemberList[(i+1)%len(MemberList)]
				des, _ = net.ResolveUDPAddr("udp", next)
				break
			}
		}
		mu.Unlock()
		// log.Println("发送心跳检查", des)
		t := time.Now()
		sendMessage(data, des)
		for {
			time.Sleep(time.Millisecond * 50)
			select {
			case <-HeartBeatChannel:
				// log.Println("心跳检查成功")
				goto Loop
			default:
				// 若没有在规定时间内收到心跳检查，则视为失败，则将des从MemberList中删除
				if time.Since(t).Milliseconds() > 200 {
					log.Printf("[%s] 心跳检查失败 %s\n", UdpAddr.String(), des.String())
					mu.Lock()
					index := -1
					for i, v := range MemberList {
						if v == des.String() {
							index = i
							break
						}
					}
					if index != -1 {
						MemberList = append(MemberList[:index], MemberList[index+1:]...)
					}
					log.Printf("[%s] 组成员列表为 %v\n", UdpAddr.String(), MemberList)
					mu.Unlock()

					go fail(des.String())
					goto Loop
				}
			}
		}
	}
}

func sendMessage(data []byte, des *net.UDPAddr) {
	socket, err := net.DialUDP("udp", nil, des)
	if err != nil {
		log.Println("连接UDP失败， err：", err, des)
		return
	}
	defer socket.Close()
	_, err = socket.Write(data)
	if err != nil {
		log.Println("发送数据失败，err：", err)
		return
	}
}

// 不断地读取，直到有指令让其退出
func listen() {
	log.Println("UDP监听开始")
	for {
		// 为了能够操控它的情况，想办法利用信道使其退出
		var data [1024]byte
		n, _, err := UDPListener.ReadFromUDP(data[:])
		if err != nil {
			log.Println("无法从UDP中读取：", err)
			continue
		}

		msg := string(data[:n])
		// fmt.Println("得到的信息是", msg)
		switch msg[:4] {
		case "join":
			// 添加组成员
			newMember := msg[5:]
			log.Println("加入新结点：", newMember)
			mu.Lock()
			MemberList = append(MemberList, newMember)
			mu.Unlock()

			// join成功要返回成功的信号
			des, _ := net.ResolveUDPAddr("udp", newMember)
			UDPListener.WriteToUDP([]byte("okok"), des)

			go add()
		case "ping": // 收到别人的心跳检查
			// log.Println("收到心跳检查了")
			des, _ := net.ResolveUDPAddr("udp", msg[5:])
			UDPListener.WriteToUDP([]byte("pong"), des)
		case "pong": // 收到别人对心跳检查的相应
			// log.Println("收到心跳检查相应了")
			HeartBeatChannel <- true
		case "addd":
			if isReferrer {
				continue
			}

			members := strings.Split(msg[5:], " ")
			mu.Lock()
			MemberList = members
			log.Printf("[%s] 加入新节点 %v\n", UdpAddr.String(), members[len(members)-1])
			log.Printf("[%s] 组成员列表为 %v\n", UdpAddr.String(), MemberList)
			mu.Unlock()

			// 传递这一条消息
			go add()
		case "fail":
			addrs := strings.Split(msg[5:], " ")
			if addrs[0] == UdpAddr.String() {
				continue
			}

			log.Printf("[%s] 有节点失败 %s\n", UdpAddr.String(), addrs[1])
			mu.Lock()
			MemberList = addrs[2:]
			log.Printf("[%s] 组成员列表为 %v\n", UdpAddr.String(), MemberList)
			mu.Unlock()

			// 转发消息
			go transmit(data[:n])
		case "left":
			addrs := strings.Split(msg[5:], " ")

			log.Printf("[%s] 有节点主动退出 %s\n", UdpAddr.String(), addrs[0])
			mu.Lock()
			MemberList = addrs[1:]
			log.Printf("[%s] 组成员列表为 %v\n", UdpAddr.String(), MemberList)
			mu.Unlock()

			go realLeave(addrs[0])
		case "leav":
			addrs := strings.Split(msg[5:], " ")
			if addrs[0] == UdpAddr.String() {
				continue
			}

			log.Printf("[%s] 有节点主动退出 %s\n", UdpAddr.String(), addrs[1])
			mu.Lock()
			MemberList = addrs[2:]
			log.Printf("[%s] 组成员列表为 %v\n", UdpAddr.String(), MemberList)
			mu.Unlock()

			go transmit(data[:n])
		case "okok":
			JoinChannel <- true
		case "abcd":
			leave()
		case "wake":
			go join()

		}
		// _, err = UDPListener.WriteToUDP(data[:n], addr) // 发送数据
		// if err != nil {
		// 	fmt.Println("Write to udp failed, err: ", err)
		// 	continue
		// }
	}
}
