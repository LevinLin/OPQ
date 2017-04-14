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

package main

import (
	//"fmt"
	"os"
	"strconv"
	"syscall"
	"unsafe"
)

var indexArray = make([]byte, 8*DefaultCmdPerFile) //maximun

type Deliverer struct {
	Topic    string
	TaskChan chan deliveryTask
	QuitChan chan bool
}

func newDeliverer(topicName string) Deliverer {
	deliverer := Deliverer{
		Topic:    topicName,
		TaskChan: make(chan deliveryTask),
		QuitChan: make(chan bool)}
	return deliverer
}

func (d *Deliverer) start() {
	go func() {
		for {
			select {
			case task := <-d.TaskChan:
				var dFile *os.File
				var iFile *os.File
				var dlvFilePath string
				var indexFilePath string
				var filePrefix string
				var dir string
				var factor uint64
				var fileIndex uint64
				var err error
				var i uint64
				var iState os.FileInfo
				var iSize int64
				var lenBlod int
				var cmd uint64
				buffer := make([]byte, unsafe.Sizeof(i))
				topicName := task.Topic

				topic, ok := topics[topicName]
				if !ok {
					Println("fetch topic failed")
					goto TAILHERE
				}

				if !topic.needRelodDelivery {
					cmd = topic.dlv
				} else { //may need to replay message, in other word we changed dlv file and set needRelodDelivery to true manually
					dir, _ = os.Getwd()
					dlvFilePath = dir + "/topics/" + topicName + "/dlv"
					dFile, err = os.OpenFile(dlvFilePath, os.O_RDWR, os.FileMode(0755))
					if nil != err {
						Println("open delivery file filed")
						goto TAILHERE
					}
					_, _ = dFile.ReadAt(buffer, 0)
					i = BytesToUint64(buffer)
					if i > 0 {
						topic.dlv = i
						cmd = i
					}
					topic.needRelodDelivery = false
				}

				factor = uint64(cmd) / DefaultCmdPerFile
				fileIndex = DefaultCmdPerFile * factor
				filePrefix = strconv.FormatUint(fileIndex, 10)
				dir, _ = os.Getwd()
				indexFilePath = dir + "/topics/" + topicName + "/" + filePrefix + ".idx"
				iFile, err = os.OpenFile(indexFilePath, os.O_RDONLY, os.FileMode(0755))
				if nil != err {
					Println("open index file failed, file path: ", indexFilePath)
					goto TAILHERE
				}
				iState, err = iFile.Stat()
				if nil != err {
					Println("get index file state failed")
					goto TAILHERE
				}
				iSize = iState.Size()
				indexArray, err = syscall.Mmap(int(iFile.Fd()), 0, int(iSize), syscall.PROT_READ, syscall.MAP_SHARED)
				topic.indexMap = indexArray[:iSize]

				// TODO
				topic.fileIndexForDelivery = fileIndex
				_ = iFile.Close()

				//for ; cmd <= topic.sto; cmd++ {
				for ; cmd < topic.sto; cmd++ {
					if topic.needRelodDelivery {
						goto TAILHERE
					}
					factor = uint64(cmd) / DefaultCmdPerFile
					fileIndex = DefaultCmdPerFile * factor
					if fileIndex != topic.fileIndexForDelivery {
						// remap index file
						filePrefix = strconv.FormatUint(fileIndex, 10)
						dir, _ = os.Getwd()
						indexFilePath = dir + "/topics/" + topicName + "/" + filePrefix + ".idx"
						iFile, err = os.OpenFile(indexFilePath, os.O_RDONLY, os.FileMode(0755))

						if nil != err {
							Println("open index file failed, file path: ", indexFilePath)
							goto TAILHERE
						}

						iState, err = iFile.Stat()
						if nil != err {
							Println("get index file state failed")
							goto TAILHERE
						}

						iSize = iState.Size()
						indexArray, err = syscall.Mmap(int(iFile.Fd()), 0, int(iSize), syscall.PROT_READ, syscall.MAP_SHARED)
						topic.indexMap = indexArray[:iSize]

						topic.fileIndexForDelivery = fileIndex

						_ = iFile.Close()
					}

					// delever commands
					cmdOffset, cmdLength := locateMessage(topic, uint32(cmd-topic.fileIndexForDelivery), 0, int64(len(topic.indexMap)-1)/8, false)
					//fmt.Println("cmdOffset: ", cmdOffset, "cmdLength: ", cmdLength, "cmd: ", cmd, "topic.dlv: ", topic.dlv, "topic.sto ", topic.sto, "fileIndex: ", topic.fileIndexForDelivery)
					if cmdOffset != 0 || cmdLength != 0 {
						Println("begin deliverMessage")
						deliverMessage(topic, cmdOffset, cmdLength, cmd)
						topic.dlv = cmd + 1
					}
				}
				topic.dlv = cmd
				topic.deliverLog.WriteAt(Uint64ToBytes(topic.dlv), 0)
				_ = dFile.Close()

			TAILHERE:
				Println("file size:", iSize, " blob len:", lenBlod)
			case <-d.QuitChan:
				Println("deliverer ", d.Topic, " quited")
				return
			}
		}
	}()
}

func (d *Deliverer) stop() {
	go func() {
		d.QuitChan <- true
	}()
}
