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
	messages "OPQ/messages"
	"bufio"
	"encoding/binary"
	//"fmt"
	flatbuffers "github.com/google/flatbuffers/go"
	CRC32 "hash/crc32"
	"io"
	//"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unsafe"
)

var (
	DefaultBufferSize        = 4096            //decide write buffer size
	DefaultIndexIntervalSize = uint32(4096)    //decide when to write an index to disk
	DefaultCmdPerFile        = uint64(1000000) //decide the amount of messages for each file (idx/msg)
	MaxHttpFieldNum          = 100
	DefaultRecorderNum       = "2"
	topics                   = map[string]*Topic{}
)

type Recorder struct {
	ID                  string
	TaskChan            chan recordTask
	GlobalTaskChanQueue chan chan recordTask
	QuitChan            chan bool
}

type Log struct {
	file   io.Writer
	buffer *bufio.Writer
	lock   *sync.Mutex
	closed bool
}

type Topic struct {
	name                 string
	cmd                  uint64 //total command
	dlv                  uint64 //delivered command
	sto                  uint64 //stored command
	needRelodDelivery    bool   //used for replaying
	indexLog             *Log
	messageLog           *Log
	commandLog           *os.File //record total number of commands which have been created
	deliverLog           *os.File //record total number of commands which have been delivered
	fileIndex            uint64   //first message index of files (idx/msg) which are being writen by recorder
	fileIndexForDelivery uint64   //first message index of file (idx) which is being read by deliverer
	indexMap             []byte   //indexs mmaped from file
	lastCmdOffset        uint32   //relative offset
	lastCmdLength        uint32
	messagesSizeCount    uint32
	recordService        *RecordService
}

func Uint64ToBytes(i uint64) []byte {
	var buf = make([]byte, unsafe.Sizeof(i))
	//bigEndian := checkMachineEndian()
	if bigEndian {
		binary.BigEndian.PutUint64(buf, i)
		Println("BigEndian", binary.BigEndian.Uint64(buf))
	} else {
		binary.LittleEndian.PutUint64(buf, i)
		Println("LittleEndian", binary.LittleEndian.Uint64(buf))
	}
	return buf
}

func BytesToUint64(s []byte) (i uint64) {
	//bigEndian := checkMachineEndian()
	if bigEndian {
		i = binary.BigEndian.Uint64(s)
	} else {
		i = binary.LittleEndian.Uint64(s)
	}
	return
}

func Uint32ToBytes(i uint32) []byte {
	var buf = make([]byte, unsafe.Sizeof(i))
	//bigEndian := checkMachineEndian()
	if bigEndian {
		binary.BigEndian.PutUint32(buf, i)
		Println("BigEndian", binary.BigEndian.Uint32(buf))
	} else {
		binary.LittleEndian.PutUint32(buf, i)
		Println("LittleEndian", binary.LittleEndian.Uint32(buf))
	}
	return buf
}

func BytesToUint32(s []byte) (i uint32) {
	//bigEndian := checkMachineEndian()
	if bigEndian {
		i = binary.BigEndian.Uint32(s)
	} else {
		i = binary.LittleEndian.Uint32(s)
	}
	return
}

func checkMachineEndian() (bigEndian bool) {
	var x int = 0x012345678
	var bp *[unsafe.Sizeof(x)]byte = (*[unsafe.Sizeof(x)]byte)(unsafe.Pointer(&x))
	if 0x01 == bp[0] {
		bigEndian = true
	} else if (0x78 & 0xff) == (bp[0] & 0xff) {
		bigEndian = false
	}
	return bigEndian
}

func (log *Log) write(message []byte) {
	Println("Log.write called [message:", message, "]")
	log.buffer.Write(message)
	//log.buffer.Flush()
}

func NewLog(file io.Writer) (log *Log) {
	log = new(Log)
	log.file = file
	log.lock = new(sync.Mutex)
	log.closed = false
	DefaultBufferSize = os.Getpagesize()
	//logDebug("default page size is " + strconv.FormatInt(int64(DefaultBufferSize), 10))
	log.buffer = bufio.NewWriterSize(file, DefaultBufferSize)
	return
}

func initTopic(name string) (err error) {
	if _, ok := topics[name]; ok {
	} else {
		dir, _ := os.Getwd()
		err = os.MkdirAll(dir+"/topics/"+name, 0755)
		if nil != err {
			Println(err.Error())
			return err
		}
		indexFilePath := dir + "/topics/" + name + "/0.idx"
		messageFilePath := dir + "/topics/" + name + "/0.msg"
		cmdFilePath := dir + "/topics/" + name + "/cmd"
		dlvFilePath := dir + "/topics/" + name + "/dlv"
		iFile, err := os.OpenFile(indexFilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
		if nil != err {
			Println("ifile path: " + indexFilePath)
			Println("open ifile failed")
			Println(err.Error())
			return err
		}
		mFile, err := os.OpenFile(messageFilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
		if nil != err {
			Println("open mfile failed")
			return err
		}
		cFile, err := os.OpenFile(cmdFilePath, os.O_RDWR|os.O_CREATE, os.FileMode(0755))
		if nil != err {
			Println("open cfile failed")
			return err
		}
		dFile, err := os.OpenFile(dlvFilePath, os.O_RDWR|os.O_CREATE, os.FileMode(0755))
		if nil != err {
			Println("open dfile failed")
			return err
		}
		iLog := NewLog(iFile)
		mLog := NewLog(mFile)
		topic := new(Topic)
		topic.name = name
		topic.indexLog = iLog
		topic.messageLog = mLog
		topic.commandLog = cFile
		topic.deliverLog = dFile
		topic.cmd = 0
		topic.dlv = 0
		topic.sto = 0
		topic.needRelodDelivery = false
		topic.fileIndex = 0
		topic.fileIndexForDelivery = 0xffffffffffffffff
		topic.lastCmdOffset = 0
		topic.lastCmdLength = 0
		topic.messagesSizeCount = 0
		topic.recordService = newRecordeService()
		topic.recordService.start()
		topic.deliverLog.WriteAt(Uint64ToBytes(0), 0)
		topics[name] = topic
	}
	return nil
}

func (topic *Topic) writeIndex(message []byte) (err error) {
	var factor uint64
	var fileIndex uint64
	factor = topic.cmd / DefaultCmdPerFile
	fileIndex = DefaultCmdPerFile * factor
	filePrefix := strconv.FormatUint(fileIndex, 10)

	//need to create new FDs for index and message file
	if topic.fileIndex != fileIndex {
		dir, _ := os.Getwd()
		indexFilePath := dir + "/topics/" + topic.name + "/" + filePrefix + ".idx"
		iFile, err := os.OpenFile(indexFilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
		if nil != err {
			return err
		}

		messageFilePath := dir + "/topics/" + topic.name + "/" + filePrefix + ".msg"
		mFile, err := os.OpenFile(messageFilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
		if nil != err {
			return err
		}

		//flush data to hard disk and release old FDs
		{
			topic.flushLogs()
			topic.commandLog.WriteAt(Uint64ToBytes(topic.cmd), 0)
			topic.sto = topic.cmd

			indexFD := (topic.indexLog.file).(*os.File)
			indexFD.Close()
			messageFD := (topic.messageLog.file).(*os.File)
			messageFD.Close()
		}

		iLog := NewLog(iFile)
		topic.indexLog = iLog
		mLog := NewLog(mFile)
		topic.messageLog = mLog

		topic.fileIndex = fileIndex
		topic.lastCmdOffset = 0
		topic.lastCmdLength = 0
	}

	var msglen uint64
	var crc32 uint32
	topic.lastCmdOffset = topic.lastCmdOffset + topic.lastCmdLength
	topic.lastCmdLength = uint32(len(message)) + uint32(unsafe.Sizeof(msglen)) + uint32(unsafe.Sizeof(crc32))
	topic.messagesSizeCount += topic.lastCmdLength
	Println("message size count: ", topic.messagesSizeCount)

	if topic.messagesSizeCount >= DefaultIndexIntervalSize {

		//cmdIndex := Uint32ToBytes(uint32(topic.cmd - fileIndex + 1))
		cmdIndex := Uint32ToBytes(uint32(topic.cmd - fileIndex))
		cmdOffset := Uint32ToBytes(topic.lastCmdOffset)
		topic.indexLog.write(cmdIndex)
		topic.indexLog.write(cmdOffset)
		topic.messagesSizeCount = 0
		//fmt.Println("cmdIndex: ", cmdIndex, " | cmdOffset: ", cmdOffset, " | message: ", message)
	}

	return nil
}

func (topic *Topic) writeMessage(message []byte) (err error) {
	msgLen := Uint64ToBytes(uint64(len(message)))
	crc32 := Uint32ToBytes(CRC32.ChecksumIEEE(message))
	Println("crc32(message) is:", crc32)

	topic.messageLog.write(msgLen)
	topic.messageLog.write(crc32)
	topic.messageLog.write(message)

	return nil
}

func newRecorder(id string, taskChanQueue chan chan recordTask) Recorder {
	recorder := Recorder{
		ID:                  id,
		TaskChan:            make(chan recordTask),
		GlobalTaskChanQueue: taskChanQueue,
		QuitChan:            make(chan bool)}
	return recorder
}

func (topic *Topic) writeLogs(message []byte) (err error) {
	//use `channel+single record service` instead of system lock
	/*
		topic.indexLog.lock.Lock()
		defer topic.indexLog.lock.Unlock()
		topic.messageLog.lock.Lock()
		defer topic.messageLog.lock.Unlock()
	*/

	//be careful here, we write index file first cause we will possiblly flush buffered data to disk in writeIndex(message)
	er := topic.writeIndex(message)
	if er != nil {
		log.Fatalf("error: %v", er)
		return er
	}

	er = topic.writeMessage(message)
	if er != nil {
		log.Fatalf("error: %v", er)
		return er
	}

	topic.cmd += 1
	return nil
}

func (topic *Topic) flushLogs() {
	topic.indexLog.buffer.Flush()
	topic.messageLog.buffer.Flush()
}

func (r *Recorder) start() {
	go func() {
		for {
			// add self to global recoder queue in every call loop
			r.GlobalTaskChanQueue <- r.TaskChan
			select {
			case task := <-r.TaskChan:
				topicName := task.Topic
				header := task.Header
				body := task.Body
				url := task.Url
				topic := topics[topicName]
				buf := prepareMessageIntoFlatBuffers(url, body, header)

				//getMessageFromFlatBuffers(buf, false)

				recordServiceTask := RecordServiceTask{Topic: topic, Record: buf}
				topic.recordService.TaskChan <- recordServiceTask
			case <-r.QuitChan:
				Println("recorder[%s] quited", r.ID)
				return
			}
		}
	}()
}

func (r *Recorder) stop() {
	go func() {
		r.QuitChan <- true
	}()
}

func prepareMessageIntoFlatBuffers(url string, body map[string]string, header map[string]string) (buf []byte) {

	Body := make([]flatbuffers.UOffsetT, MaxHttpFieldNum)
	Header := make([]flatbuffers.UOffsetT, MaxHttpFieldNum)
	var i = 0
	body_len := 0
	header_len := 0
	builder := flatbuffers.NewBuilder(0)

	url_pos := builder.CreateString(url)

	for k, v := range header {
		if i < 100 {
			header_key_pos := builder.CreateString(k)
			header_val_pos := builder.CreateString(v)
			messages.HeaderStart(builder)
			messages.HeaderAddKey(builder, header_key_pos)
			messages.HeaderAddValue(builder, header_val_pos)
			Header[i] = messages.HeaderEnd(builder)
			i++
		} else {
			//TO DO: log
		}
	}
	header_len = i
	i = 0

	for k, v := range body {
		if i < 100 {
			body_key_pos := builder.CreateString(k)
			body_val_pos := builder.CreateString(v)
			messages.BodyStart(builder)
			messages.BodyAddKey(builder, body_key_pos)
			messages.BodyAddValue(builder, body_val_pos)
			Body[i] = messages.BodyEnd(builder)
			i++
		} else {
			//TO DO: log
		}
	}
	body_len = i
	i = 0

	messages.MessageStartHeaderVector(builder, header_len)
	for j := header_len - 1; j >= 0; j-- {
		builder.PrependUOffsetT(Header[j])
	}
	headers_pos := builder.EndVector(header_len)

	messages.MessageStartBodyVector(builder, body_len)
	for j := body_len - 1; j >= 0; j-- {
		builder.PrependUOffsetT(Body[j])
	}
	bodys_pos := builder.EndVector(body_len)

	messages.MessageStart(builder)
	messages.MessageAddBody(builder, bodys_pos)
	messages.MessageAddHeader(builder, headers_pos)
	messages.MessageAddUrl(builder, url_pos)
	message_pos := messages.MessageEnd(builder)

	// finish the write operations by our User the root object:
	builder.Finish(message_pos)

	buf = builder.FinishedBytes()
	Println(buf)

	// return the byte slice containing encoded data:
	//return buf, body_len, header_len
	return buf
}

/*
func getMessageFromFlatBuffers(buf []byte, needDelivery bool, topic *Topic, cmd uint64) {

	var res *http.Response
	var newReq *http.Request
	var err error

	Message := messages.GetRootAsMessage(buf, 0)

	// We need a `messages.Body` to pass into `messages.Body()` to capture the output of the function
	Body := new(messages.Body)
	Header := new(messages.Header)

	v := url.Values{}
	rawUrl, _ := url.Parse(string(Message.Url()))
	strUrl := strings.TrimSpace(rawUrl.String()) //to remove useless spaces

	blen := Message.BodyLength()
	hlen := Message.HeaderLength()

	for i := 0; i < blen; i++ {
		if true == Message.Body(Body, i) {
			key := Body.Key()
			val := Body.Value()
			v.Add(string(key), string(val))
		}
	}

	newReq, err = http.NewRequest("POST", strUrl, strings.NewReader(v.Encode()))

	for i := 0; i < hlen; i++ {
		if true == Message.Header(Header, i) {
			key := Header.Key()
			val := Header.Value()
			if needDelivery {
				newReq.Header.Add(string(key), string(val))
			}
		}
	}

	if nil == err {
		if needDelivery {

			//res, err = http.DefaultClient.Do(newReq)

			//set request time out
			const requestTimeout = 30
			var netClientWithTimeout = &http.Client{
				Timeout: requestTimeout * time.Second,
			}
			res, err = netClientWithTimeout.Do(newReq)

			//repeat delivery and warning when fail
			for ((nil != err) || (res.StatusCode != 200)) && !topic.needRelodDelivery {
				res, err = netClientWithTimeout.Do(newReq)
				if nil == err {
					res.Body.Close()
				}
				logWarning("delivery topic[" + topic.name + "].cmd[" + strconv.FormatUint(cmd, 10) + "] failed, " + "url[" + string(strUrl) + "]")
			}
			if nil == err {
				res.Body.Close()
			}
		}
	}
}
*/

func getMessageFromFlatBuffers(buf []byte, needDelivery bool, topic *Topic, cmd uint64) {
	var err error

	Message := messages.GetRootAsMessage(buf, 0)

	// We need a `messages.Body` to pass into `messages.Body()` to capture the output of the function
	Body := new(messages.Body)
	Header := new(messages.Header)

	rawUrl, _ := url.Parse(string(Message.Url()))
	strUrl := strings.TrimSpace(rawUrl.String()) //to remove useless spaces

	if needDelivery {
		err = doReq("POST", strUrl, Message, Body, Header)

		for (nil != err) && !topic.needRelodDelivery {
			logWarning("delivery topic[" + topic.name + "].cmd[" + strconv.FormatUint(cmd, 10) + "] failed, " + "url[" + string(strUrl) + "]")
			err = doReq("POST", strUrl, Message, Body, Header)
		}
	}
}

func doReq(Method string, Url string, Message *messages.Message, Body *messages.Body, Header *messages.Header) error {
	var res *http.Response
	var newReq *http.Request
	var err error

	v := url.Values{}
	blen := Message.BodyLength()
	hlen := Message.HeaderLength()

	for i := 0; i < blen; i++ {
		if true == Message.Body(Body, i) {
			key := Body.Key()
			val := Body.Value()
			v.Add(string(key), string(val))
		}
	}

	newReq, err = http.NewRequest(Method, Url, strings.NewReader(v.Encode()))

	for i := 0; i < hlen; i++ {
		if true == Message.Header(Header, i) {
			key := Header.Key()
			val := Header.Value()
			newReq.Header.Add(string(key), string(val))
		}
	}

	if nil == err {
		//set request time out
		const requestTimeout = 30
		var netClientWithTimeout = &http.Client{
			Timeout: requestTimeout * time.Second,
		}

		res, err = netClientWithTimeout.Do(newReq)

		if nil == err {
			res.Body.Close()
		} else {
			return err
		}
	} else {
		return err
	}
	return nil
}

func getMessageFromMessageFileContent(buf []byte) {
	Message := messages.GetRootAsMessage(buf, 0)
	// We need a `messages.Body` to pass into `messages.Body()` to capture the output of the function
	Body := new(messages.Body)
	Header := new(messages.Header)

	url := Message.Url()
	if nil != url {
		Println("url: " + string(url))
	}

	for i := 0; true == Message.Header(Header, i); i++ {
		key := Header.Key()
		val := Header.Value()
		Println("header_key: "+string(key), "header_val: "+string(val))
	}

	for j := 0; true == Message.Body(Body, j); j++ {
		key := Body.Key()
		val := Body.Value()
		Println("body_key: "+string(key), "body_val: "+string(val))
	}
}
