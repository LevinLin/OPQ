/**
MIT License

Copyright (c) 2017 levin.lin (vllm@163.com)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

     ██████╗ ██████╗  ██████╗
    ██╔═══██╗██╔══██╗██╔═══██╗
    ██║   ██║██████╔╝██║   ██║  ╔══════╗  █                       █
    ██║   ██║██╔═══╝ ██║▄▄ ██║ ╔╝ ▮███ ╚╗ █   ■■  █  █  ■   ■■    █   ■   ■■
    ╚██████╔╝██║     ╚██████╔╝ ║ ▮█     ║ █  █  █ █ █  ■█  █  █   █  ■█  █  █
     ╚═════╝ ╚═╝      ╚══▀▀═╝  ║ ▮█     ║ █  █■■  █ █   █  █  █   █   █  █  █
                               ╚╗ ▮███ ╔╝ ██  ■■   █    ██ █  █ ■ ██  ██ █  █
                                ╚══════╝
*/

// recordService
package main

import (
	//"fmt"
	CRC32 "hash/crc32"
	"os"
	"strconv"
	"time"
	"unsafe"
)

var (
	DefaultWriteBufferCount   = 4 //when it is set to 4, performance result is 32000(2KB payload)msgs/s and 22000(2KB payload)msgs/s
	DefaultChannelBufferCount = 4
	MessageCountForTest       = 0
	MessageTotalForTest       = 0
)

type RecordServiceTask struct {
	Topic  *Topic
	Record []byte
}

type RecordService struct {
	TaskChan    chan RecordServiceTask
	QuitChan    chan bool
	BufferCount int
	BeginTime   int64 //for peformance test
	EndTime     int64 //for peformance test
}

func newRecordeService() (rs *RecordService) {
	rs = new(RecordService)
	rs.TaskChan = make(chan RecordServiceTask, DefaultChannelBufferCount)
	rs.QuitChan = make(chan bool)
	rs.BufferCount = 0
	rs.BeginTime = 0
	rs.EndTime = 0
	return
}

func testFileContent(topic *Topic) {
	fileIndex := topic.fileIndex
	filePrefix := strconv.FormatUint(fileIndex, 10)
	dir, _ := os.Getwd()
	messageFilePath := dir + "/topics/" + topic.name + "/" + filePrefix + ".msg"
	pos := topic.lastCmdOffset
	leng := topic.lastCmdLength
	var i uint64
	var j uint32
	msglen := make([]byte, unsafe.Sizeof(i))
	crc32 := make([]byte, unsafe.Sizeof(j))
	message := make([]byte, leng-uint32(unsafe.Sizeof(i))-uint32(unsafe.Sizeof(j)))

	f, err := os.Open(messageFilePath)
	defer f.Close()
	var crc32sum uint32 = 0
	if nil == err {
		_, err := f.Seek(int64(pos), 0)
		if nil == err {
			_, err := f.Read(msglen)
			if nil == err {
				msgLen := BytesToUint64(msglen)
				Println("message length:", msgLen)
			}
			_, err = f.Read(crc32)
			if nil == err {
				crc32sum = BytesToUint32(crc32)
				Println("crc32 checksum:", crc32sum)
			}
			//_, err = f.Seek(int64(pos+uint32(unsafe.Sizeof(i))), 0)
			_, err = f.Read(message)
			if nil == err {
				//Println("test content after flush:")
				//Println(string(buf))
				crc32check := CRC32.ChecksumIEEE(message)
				Println("crc32 actual checksum:", crc32check)

				Println("actual message length:", len(message))
				if crc32sum == crc32check {
					Println("decoded content:")
					//getMessageFromFlatBuffers(message, false)
					logDebug("decode content from file succeeded")
				} else {
					warn := Sprintf("crc32 check sum from file is %d, but actual check sum is %d", crc32sum, crc32check)
					logWarning(warn)
				}
			}
		}
	}
}

func deliverMessage(topic *Topic, cmdOffset uint32, cmdLength uint32, cmd uint64) {
	fileIndex := topic.fileIndexForDelivery
	filePrefix := strconv.FormatUint(fileIndex, 10)
	dir, _ := os.Getwd()
	messageFilePath := dir + "/topics/" + topic.name + "/" + filePrefix + ".msg"
	pos := cmdOffset
	leng := cmdLength
	var i uint64
	var j uint32
	msglen := make([]byte, unsafe.Sizeof(i))
	crc32 := make([]byte, unsafe.Sizeof(j))
	message := make([]byte, leng-uint32(unsafe.Sizeof(i))-uint32(unsafe.Sizeof(j)))

	f, err := os.Open(messageFilePath)
	defer f.Close()
	var crc32sum uint32 = 0
	if nil == err {
		_, err := f.Seek(int64(pos), 0)
		if nil == err {
			_, err := f.Read(msglen)
			if nil == err {
				_ = BytesToUint64(msglen)
			}
			_, err = f.Read(crc32)
			if nil == err {
				crc32sum = BytesToUint32(crc32)
			}
			_, err = f.Read(message)
			if nil == err {
				crc32check := CRC32.ChecksumIEEE(message)
				if crc32sum == crc32check {
					getMessageFromFlatBuffers(message, true, topic, cmd)
				} else {
					warn := Sprintf("crc32 check sum from file is %d, but actual check sum is %d", crc32sum, crc32check)
					logWarning(warn)
				}
			}
		}
	}
}

func (s *RecordService) start() {
	s.BeginTime = time.Now().UnixNano() / (1000 * 1000) //ms
	go func() {
		for {
			select {
			case task := <-s.TaskChan:
				topic := task.Topic
				buf := task.Record
				_ = topic.writeLogs(buf)
				s.BufferCount++
				MessageCountForTest++
				MessageTotalForTest++
				if s.BufferCount >= DefaultWriteBufferCount {
					s.BufferCount = 0
					topic.flushLogs()
					topic.commandLog.WriteAt(Uint64ToBytes(topic.cmd), 0)
					topic.sto = topic.cmd //track stored command total number

					//log for performance test
					if MessageCountForTest >= 100000 {
						s.EndTime = time.Now().UnixNano() / (1000 * 1000)
						CostTime := s.EndTime - s.BeginTime
						str := Sprintf("total %d messages stored, total %s ms costed", MessageTotalForTest, strconv.FormatInt(CostTime, 10))
						logDebug(str)
						MessageCountForTest = 0
					}

					//notify deliverrer
					deliverrer := initDeliverer(topic.name)
					dlvTask := deliveryTask{Topic: topic.name}
					deliverrer.TaskChan <- dlvTask

					/*
						var i uint64
						buffer := make([]byte, unsafe.Sizeof(i))
						_, _ = topic.commandLog.ReadAt(buffer, 0)
						cmd := BytesToUint64(buffer)
						testFileContent(topic)
						Println("total commands: ", cmd)
					*/
				}
			case <-s.QuitChan:
				return
			}
		}
	}()
}

func (s *RecordService) stop() {
	go func() {
		s.QuitChan <- true
	}()
}
