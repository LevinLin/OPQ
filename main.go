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
	"flag"
	//"fmt"
	"github.com/fvbock/endless"
	"github.com/gorilla/mux"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	//_ "net/http/pprof"
	"net/url"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unsafe"
)

var (
	sysLogName       = flag.String("syslog", "system.log", "System log name")
	sysPort          = flag.String("port", "8999", "Listening port, default to 8999")
	debugModel       = flag.String("debug", "no", "System runs in debug model when given debug=yes, default to no")
	enableAdmin      = flag.String("admin", "no", "Enable admin portal when given admin=yes, default to no")
	bigEndian   bool = false
)

const form = `<html>
			  		<body>
						<form action="#" method="POST" name="bar">
							<input type="text" name="input" />
							<input type="submit" value="Submit" />
						</form>
					</body>
			</html>`

const opqForm = `<html>
			  		<body>
						<form action="opq/push" method="POST" name="bar">
							<input type="text" name="url" />
							<input type="text" name="topic" />
							<input type="submit" value="Submit" />
						</form>
					</body>
			</html>`

const testReadForm = `<html>
			  		<body>
						<form action="opq/replay" method="POST" name="bar">
							<input type="text" name="topic" />
							<input type="text" name="cmd" />
							<input type="submit" value="Submit" />
						</form>
					</body>
			</html>`

type StatFrame struct {
	Time    int64 `json:"time"`
	Latency int   `json:"latency"`
	Reqs    int   `json:"reqs"`
}

func main() {
	bigEndian = checkMachineEndian()
	runtime.GOMAXPROCS(runtime.NumCPU())
	flag.Parse()
	uid := os.Getuid()
	euid := os.Geteuid()
	gid := os.Getgid()
	egid := os.Getegid()
	Printf("uid[%d] gid[%d] euid[%d] egid[%d]\n", uid, gid, euid, egid)

	/*
		// for pprof
		go func() {
			log.Println(http.ListenAndServe(":6060", nil))
		}()
	*/

	_, _ = initSystemLogs()
	_ = recoverTopics()

	/*
		http.HandleFunc("/haha", handleRequest)
		http.HandleFunc("/opq/push", listenForNewRequest)
		http.HandleFunc("/opq/replay", listenForReplayRequest)

		if err := http.ListenAndServe(":"+*sysPort, nil); err != nil {
			Println("error happens")
			panic(err)
		}
	*/

	mux1 := mux.NewRouter()
	mux1.HandleFunc("/haha", handleRequest).Methods("GET")
	mux1.HandleFunc("/opq/push", listenForNewRequest).Methods("POST")
	mux1.HandleFunc("/opq/replay", listenForReplayRequest).Methods("POST")

	if "yes" == *enableAdmin {
		setupAdminReport()
	}

	/*
		if err := endless.ListenAndServe(":"+*sysPort, mux1); err != nil {
			Println("error happens")
			panic(err)
		}
	*/

	/*
		err := endless.ListenAndServe(":"+*sysPort, mux1)
	*/

	srv := endless.NewServer(":"+*sysPort, mux1)
	srv.SignalHooks[endless.PRE_SIGNAL][syscall.SIGHUP] = append(
		srv.SignalHooks[endless.PRE_SIGNAL][syscall.SIGHUP],
		flushMessageBeforeQuit)
	srv.SignalHooks[endless.PRE_SIGNAL][syscall.SIGTERM] = append(
		srv.SignalHooks[endless.PRE_SIGNAL][syscall.SIGTERM],
		flushMessageBeforeQuit)
	srv.SignalHooks[endless.PRE_SIGNAL][syscall.SIGINT] = append(
		srv.SignalHooks[endless.PRE_SIGNAL][syscall.SIGINT],
		flushMessageBeforeQuit)
	srv.SignalHooks[endless.PRE_SIGNAL][syscall.SIGTSTP] = append(
		srv.SignalHooks[endless.PRE_SIGNAL][syscall.SIGTSTP],
		flushMessageBeforeQuit)
	err := srv.ListenAndServe()

	if err != nil {
		log.Println(err)
	}
	logDebug("Server on " + *sysPort + " stopped")

	os.Exit(0)
}

func flushMessageBeforeQuit() {
	for _, v := range topics {
		v.recordService.stop() // stop writting any new log
		deliverer, ok := delivererPerTopic[v.name]
		if ok {
			deliverer.stop() // stop deliverring any message to target
		}

		v.flushLogs()                                 // flush index/message log to disk
		v.commandLog.WriteAt(Uint64ToBytes(v.cmd), 0) // flush total cmd count to disk
		v.deliverLog.WriteAt(Uint64ToBytes(v.dlv), 0) // flush deliverred cmd count to disk
	}
}

func setupAdminReport() { // useless right now
	ticker := time.NewTicker(time.Millisecond * 800)
	tickCome := ticker.C
	tickStop := make(chan bool)
	tickCount := 0
	reqCount := 0
	latencyCount := 0
	SimulationStart := time.Now()
	go func() {
		time.Sleep(time.Second * 80)
		tickStop <- true
	}()
	go StartWsServer()
	go func() {
		for {
			select {
			case <-tickCome:
				tickCount++
				Println("tick", tickCount)
				//Println("tick", tickCount)
				latencyCount += (rand.Int() % 100) * 10
				reqCount += rand.Int() % 100
				statFrame := StatFrame{
					time.Since(SimulationStart).Nanoseconds() / 1000000000,
					latencyCount,
					reqCount,
				}
				BroadcastStatFrame(statFrame)
			case <-tickStop:
				ticker.Stop()
				for index, wsConn := range connectionRegistry {

					wsConn.Close()
					Remove(index)
				}
				return
			}
		}
	}()
}

func recoverTopics() (err error) {
	cur_dir, _ := os.Getwd()
	cur_path := cur_dir + "/topics/"
	dir, err := ioutil.ReadDir(cur_path)
	if err != nil {
		//TODO loging
		return err
	}
	for _, file := range dir {
		if file.IsDir() {
			_, err = ioutil.ReadDir(cur_path + file.Name() + "/")
			if err != nil {
				//TODO loging
				continue
			}

			// cmd file must exists, else all path will be removed for consistence
			cmdFilePath := cur_path + file.Name() + "/cmd"
			cFile, err := os.OpenFile(cmdFilePath, os.O_RDWR, os.FileMode(0755))
			if nil != err {
				//TODO loging
				os.RemoveAll(cur_path + file.Name())
				continue
			}
			var i uint64
			buffer := make([]byte, unsafe.Sizeof(i))
			_, _ = cFile.ReadAt(buffer, 0)
			totalCmd := BytesToUint64(buffer)
			if totalCmd <= 0 {
				//TODO loging
				os.RemoveAll(cur_path + file.Name())
				continue
			}

			logDebug("total " + strconv.FormatUint(totalCmd, 10) + " commands in topic " + file.Name())

			factor := totalCmd / DefaultCmdPerFile
			fileIndex := DefaultCmdPerFile * factor
			filePrefix := strconv.FormatUint(fileIndex, 10)
			idxFilePath := cur_path + file.Name() + "/" + filePrefix + ".idx"
			iFile, err := os.OpenFile(idxFilePath, os.O_WRONLY|os.O_APPEND|os.O_CREATE, os.FileMode(0755))
			if nil != err {
				//TODO loging
				Println("open index file failed")
				continue
			}
			msgFilePath := cur_path + file.Name() + "/" + filePrefix + ".msg"
			mFile, err := os.OpenFile(msgFilePath, os.O_WRONLY|os.O_APPEND, os.FileMode(0755))
			if nil != err {
				//TODO loging
				continue
			}
			dlvFilePath := cur_path + file.Name() + "/dlv"
			dFile, err := os.OpenFile(dlvFilePath, os.O_RDWR|os.O_CREATE, os.FileMode(0755))
			if nil != err {
				//TODO loging
				continue
			}
			iLog := NewLog(iFile)
			mLog := NewLog(mFile)
			topic := new(Topic)
			topic.name = file.Name()
			topic.indexLog = iLog
			topic.messageLog = mLog
			topic.commandLog = cFile
			topic.deliverLog = dFile
			topic.cmd = totalCmd
			_, _ = dFile.ReadAt(buffer, 0)
			topic.dlv = BytesToUint64(buffer)
			topic.sto = totalCmd
			topic.needRelodDelivery = false
			topic.fileIndex = fileIndex
			factor = topic.dlv / DefaultCmdPerFile
			fileIndex = DefaultCmdPerFile * factor
			topic.fileIndexForDelivery = fileIndex

			iFile, err = os.OpenFile(idxFilePath, os.O_RDONLY, os.FileMode(0755))
			if nil != err {
				Println("open index file failed, file path: ", idxFilePath)
				continue
			}
			iState, err := iFile.Stat()
			if nil != err {
				//TODO loging
				Println("iFile.Stat failed")
				continue
			}
			iSize := iState.Size()

			if iSize > 0 {
				indexArray, err := syscall.Mmap(int(iFile.Fd()), 0, int(iSize), syscall.PROT_READ, syscall.MAP_SHARED)
				if nil != err {
					//TODO loging
					Println("mmap index file failed")
					Println(err)
					continue
				}
				topic.indexMap = indexArray[:iSize]

				/*
					Println("index content begin---------------------")
					var t int64 = 0
					for ; t < iSize; t += 8 {
						tmp := topic.indexMap[t : t+4]
						cmdNo := BytesToUint32(tmp)
						tmp = topic.indexMap[t+4 : t+8]
						cmdOffset := BytesToUint32(tmp)
						Print(" [cmdNo]: ", cmdNo, " [cmdOffset]: ", cmdOffset)
					}
					Println("index content end---------------------")
				*/
			}
			lastCmdOffset, lastCmdLength := locateMessage(topic, uint32(totalCmd-fileIndex-1), 0, (iSize-1)/8, true)
			topic.lastCmdOffset = lastCmdOffset
			topic.lastCmdLength = lastCmdLength
			//fmt.Println("recovered: topic: ", topic.name, " | totalCmd: ", totalCmd, " | topic.lastCmdOffset: ", topic.lastCmdOffset, " | topic.lastCmdLength: ", topic.lastCmdLength)
			//testFileContent(topic)
			topic.messagesSizeCount = 0

			//Println(topic.lastCmdOffset)
			//Println(topic.lastCmdLength)
			topic.recordService = newRecordeService()
			topic.recordService.start()
			topics[file.Name()] = topic
		}
	}
	return nil
}

func linearSearch(topic *Topic, cmdOffsetBegin uint32, cmdNoBegin uint32, cmdNoAaim uint32, forRecovery bool) (cmdOffset uint32, cmdLength uint32, err error) {
	var fileIndex uint64
	var i uint64
	var j uint32
	var msgLength uint64 = 0

	if forRecovery {
		fileIndex = topic.fileIndex
	} else {
		fileIndex = topic.fileIndexForDelivery
	}
	filePrefix := strconv.FormatUint(fileIndex, 10)
	dir, _ := os.Getwd()
	messageFilePath := dir + "/topics/" + topic.name + "/" + filePrefix + ".msg"
	msglen := make([]byte, unsafe.Sizeof(i))
	f, err := os.Open(messageFilePath)
	defer f.Close()

	cmdNo := cmdNoBegin
	nextCmdOffset := cmdOffsetBegin
	//fmt.Print("linearSearch(): cmdNoBegin: ", cmdNoBegin, " | cmdNoAaim: ", cmdNoAaim)
	for ; cmdNo < cmdNoAaim; cmdNo++ {
		_, err := f.Seek(int64(nextCmdOffset), 0)
		msgLength = 0
		if nil == err {
			_, err := f.Read(msglen)
			if nil == err {
				msgLength = BytesToUint64(msglen)
				nextCmdOffset += uint32(msgLength) + uint32(unsafe.Sizeof(i)) + uint32(unsafe.Sizeof(j))
				//fmt.Print(" | msgLength: ", msgLength, " | nextCmdOffset: ", nextCmdOffset)
			} else {
				return 0, 0, err
			}
		} else {
			return 0, 0, err
		}
	}
	_, err = f.Seek(int64(nextCmdOffset), 0)
	if nil == err {
		_, err := f.Read(msglen)
		if nil == err {
			msgLength = BytesToUint64(msglen)
			return nextCmdOffset, uint32(msgLength) + uint32(unsafe.Sizeof(i)) + uint32(unsafe.Sizeof(j)), nil
		} else {
			return 0, 0, err
		}
	} else {
		return 0, 0, err
	}
}

/**
 * binary search index for message
 */
func locateMessage(topic *Topic, cmdIdx uint32, begin int64, end int64, forRecovery bool) (cmdOffset uint32, cmdLength uint32) {
	var cmdNoBeg uint32
	var cmdNoMid uint32
	var cmdNoEnd uint32
	var cmdOffsetBeg uint32
	var cmdOffsetMid uint32
	var cmdOffsetEnd uint32
	var fileIndex uint64
	var i uint64
	var j uint32
	var msgLength uint64 = 0

	if forRecovery {
		fileIndex = topic.fileIndex
	} else {
		fileIndex = topic.fileIndexForDelivery
	}
	filePrefix := strconv.FormatUint(fileIndex, 10)
	dir, _ := os.Getwd()
	messageFilePath := dir + "/topics/" + topic.name + "/" + filePrefix + ".msg"
	msglen := make([]byte, unsafe.Sizeof(i))
	f, err := os.Open(messageFilePath)
	defer f.Close()
	if nil != err {
		return 0, 0
	}

	/*
		indexFilePath := dir + "/topics/" + topic.name + "/" + filePrefix + ".idx"
		index := make([]byte, unsafe.Sizeof(j))
		//idxf, err := os.Open(indexFilePath)
		iFile, err := os.OpenFile(indexFilePath, os.O_RDONLY, os.FileMode(0755))
		defer iFile.Close()
		if nil == err {
			fileInfo, err := iFile.Stat()
			if nil == err {
				fileSize := fileInfo.Size()
				var idx int64 = 0
				for ; idx <= int64(fileSize)-int64(unsafe.Sizeof(j)*2); idx += int64(unsafe.Sizeof(j) * 2) {
					_, err = iFile.Seek(idx, 0)
					_, err = iFile.Read(index)
					j = BytesToUint32(index)
					fmt.Print("cmdIdx: ", j, " ")
					_, err = iFile.Seek(idx+int64(unsafe.Sizeof(j)), 0)
					_, err = iFile.Read(index)
					j = BytesToUint32(index)
					fmt.Print("cmdOffset: ", j, " | ")
				}
				fmt.Println("\n")
			}
		}
	*/

	middle := int64((begin + end) / 2)

	if len(topic.indexMap) != 0 {
		tmp := topic.indexMap[(begin * 8):(begin*8 + 4)]
		cmdNoBeg = BytesToUint32(tmp)
		tmp = topic.indexMap[(begin*8 + 4):(begin*8 + 8)]
		cmdOffsetBeg = BytesToUint32(tmp)

		tmp = topic.indexMap[(middle * 8):(middle*8 + 4)]
		cmdNoMid = BytesToUint32(tmp)
		tmp = topic.indexMap[(middle*8 + 4):(middle*8 + 8)]
		cmdOffsetMid = BytesToUint32(tmp)

		tmp = topic.indexMap[(end * 8):(end*8 + 4)]
		cmdNoEnd = BytesToUint32(tmp)
		tmp = topic.indexMap[(end*8 + 4):(end*8 + 8)]
		cmdOffsetEnd = BytesToUint32(tmp)

		//fmt.Println("cmdNoBeg: ", cmdNoBeg, "cmdOffsetBeg: ", cmdOffsetBeg, "cmdNoMid: ", cmdNoMid, "cmdOffsetMid: ", cmdOffsetMid, "cmdNoEnd: ", cmdNoEnd, "cmdOffsetEnd: ", cmdOffsetEnd)

		if cmdIdx == cmdNoBeg {
			//fmt.Println("cmdIdx: ", cmdIdx, "== cmdNoBeg: ", cmdNoBeg)
			_, err := f.Seek(int64(cmdOffsetBeg), 0)
			if nil == err {
				_, err := f.Read(msglen)
				if nil == err {
					msgLength = BytesToUint64(msglen)
					return cmdOffsetBeg, uint32(msgLength) + uint32(unsafe.Sizeof(i)) + uint32(unsafe.Sizeof(j))
				}
			}
			return 0, 0
		}

		if cmdIdx == cmdNoMid {
			//fmt.Println("cmdIdx: ", cmdIdx, "== cmdNoMid: ", cmdNoMid)
			_, err := f.Seek(int64(cmdOffsetMid), 0)
			if nil == err {
				_, err := f.Read(msglen)
				if nil == err {
					msgLength = BytesToUint64(msglen)
					return cmdOffsetMid, uint32(msgLength) + uint32(unsafe.Sizeof(i)) + uint32(unsafe.Sizeof(j))
				}
			}
			return 0, 0
		}

		if cmdIdx == cmdNoEnd {
			//fmt.Println("cmdIdx: ", cmdIdx, "== cmdNoEnd: ", cmdNoEnd)
			_, err := f.Seek(int64(cmdOffsetEnd), 0)
			if nil == err {
				_, err := f.Read(msglen)
				if nil == err {
					msgLength = BytesToUint64(msglen)
					return cmdOffsetEnd, uint32(msgLength) + uint32(unsafe.Sizeof(i)) + uint32(unsafe.Sizeof(j))
				}
			}
			return 0, 0
		}

		if cmdIdx < cmdNoBeg {
			//fmt.Println("cmdIdx: ", cmdIdx, "< cmdNoBeg: ", cmdNoBeg)
			cmdOffset, cmdLength, err := linearSearch(topic, 0, 0, cmdIdx, forRecovery)
			if nil == err {
				return cmdOffset, cmdLength
			}
			return 0, 0
		}

		if cmdIdx > cmdNoEnd {
			//fmt.Println("cmdIdx: ", cmdIdx, "> cmdNoEnd: ", cmdNoEnd)
			cmdOffset, cmdLength, err := linearSearch(topic, cmdOffsetEnd, cmdNoEnd, cmdIdx, forRecovery)
			if nil == err {
				return cmdOffset, cmdLength
			}
			return 0, 0
		}

		if cmdOffsetBeg == cmdOffsetMid {
			//fmt.Println("cmdIdx: ", cmdIdx, "== cmdOffsetMid: ", cmdOffsetMid)
			cmdOffset, cmdLength, err := linearSearch(topic, cmdOffsetBeg, cmdNoBeg, cmdIdx, forRecovery)
			if nil == err {
				return cmdOffset, cmdLength
			}
			return 0, 0
		}

		if cmdIdx < cmdNoMid { // cmdIdx ∈ (map(begin), map(middle))
			//fmt.Println("cmdIdx: ", cmdIdx, "< cmdNoMid: ", cmdNoMid)
			return locateMessage(topic, cmdIdx, begin, middle, forRecovery)
		} else { // cmdIdx ∈ (map(middle), map(end))
			//fmt.Println("cmdIdx: ", cmdIdx, "> cmdNoMid: ", cmdNoMid)
			if middle == int64((middle+end)/2) {
				cmdOffset, cmdLength, err := linearSearch(topic, cmdOffsetMid, cmdNoMid, cmdIdx, forRecovery)
				if nil == err {
					return cmdOffset, cmdLength
				}
				return 0, 0
			} else {
				return locateMessage(topic, cmdIdx, middle, end, forRecovery)
			}
		}
	} else {
		//fmt.Println("no index map")
		cmdOffset, cmdLength, err := linearSearch(topic, 0, 0, cmdIdx, forRecovery)
		if nil == err {
			return cmdOffset, cmdLength
		}
		return 0, 0
	}
}

func initSystemLogs() (file *os.File, err error) {
	dir, _ := os.Getwd()
	err = os.MkdirAll(dir+"/logs/", 0755)
	if nil != err {
		Println(err.Error())
		return nil, err
	}
	sysLogPath := dir + "/logs/" + *sysLogName
	sysLogFile, err := os.OpenFile(sysLogPath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0666)
	if nil != err {
		Println(err.Error())
		return nil, err
	}
	log.SetOutput(sysLogFile)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	return sysLogFile, nil
}

func logWarning(message string) {
	log.SetPrefix("[warning]")
	log.Println(message)
}

func logDebug(message string) {
	log.SetPrefix("[debug]")
	log.Println(message)
}

func logError(message string) {
	log.SetPrefix("[error]")
	log.Println(message)
}

func checkError(err error) {
	if err != nil {
		log.Fatalf("error: %v", err)
	}
}

func handleRequest(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		io.WriteString(w, opqForm)

	case "POST":
		inputValue := req.FormValue("input")
		formValues := req.Form.Encode()
		v := url.Values{}
		v.Set("input", inputValue)
		v.Add("name", "levin")

		newReq, err := http.NewRequest("POST", "http://localhost:8099/login.php", strings.NewReader(v.Encode()))
		checkError(err)

		// set header for new request
		for k, v := range req.Header {
			for _, vv := range v {
				newReq.Header.Add(k, vv)
			}
		}
		if host := req.Header.Get("Host"); host != "" {
			newReq.Host = host
		}

		// do the request
		res, err := http.DefaultClient.Do(newReq)
		checkError(err)
		defer res.Body.Close()

		// get data from original reponse
		data, err := ioutil.ReadAll(res.Body)
		checkError(err)

		// set header for passby response
		for k, v := range res.Header {
			for _, vv := range v {
				w.Header().Add(k, vv)
			}
		}

		// return response
		Println(formValues)
		Println("ahaha")
		w.WriteHeader(res.StatusCode)
		w.Write([]byte(formValues))
		w.Write(data)

	}
}
