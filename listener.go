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
	"io"
	"net/http"
	"net/url"
	"runtime"
	"strconv"
	"strings"
)

var recordTaskQueue = make(chan recordTask, 4096)
var recordTaskChanQueuePerTopic = map[string]chan chan recordTask{}
var delivererPerTopic = map[string]Deliverer{}

func listenForNewRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		io.WriteString(w, opqForm)

	case "POST":

		/**
		 *If you make multipart/form-data POST request,
		 *ParseForm does not parse request body correctly (this might be a bug).
		 *So, use ParseMultipartForm if that is the case.
		**/
		//r.ParseForm()
		_ = r.ParseMultipartForm(10 * 1024 * 1024)
		frm := r.Form
		hd := r.Header
		var body map[string]string
		var header map[string]string
		header = make(map[string]string)
		body = make(map[string]string)

		if _, ok := frm["topic"]; !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("BadRequest: " + frm.Encode()))
			goto TAIL
		}
		if _, ok := frm["url"]; !ok {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("BadRequest: " + frm.Encode()))
			goto TAIL
		}
		var recorder string
		DefaultRecorderNum = strconv.FormatInt(int64(runtime.NumCPU()*2-1), 10)
		//logDebug("recorder number for topic[" + frm["topic"][0] + "] is " + DefaultRecorderNum)
		if recorders, ok := frm["recorder"]; ok {
			recorder = recorders[0]
		} else {
			recorder = DefaultRecorderNum
		}
		recorderNum, err := strconv.Atoi(recorder)
		if nil != err {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("BadRequest: " + frm.Encode()))
			goto TAIL
		}

		topicNames, _ := frm["topic"]
		urls, _ := frm["url"]
		topicName := topicNames[0]
		//url := urls[0]
		rawUrl, _ := url.Parse(urls[0])
		strUrl := strings.TrimSpace(rawUrl.String()) //to remove useless spaces

		for k, v := range frm {
			if len(v) == 1 {
				Println(k, v[0])
				body[k] = v[0]
			} else {
				Println(k, v)
			}
		}

		for k, v := range hd {
			if len(v) == 1 {
				Println(k, v[0])
				header[k] = v[0]
			} else {
				Println(k, v)
			}
		}

		Println(topicName)
		Println(recorderNum)

		err = initTopic(topicName)
		if nil != err {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("InternalServerError"))
			goto TAIL
		}

		initRecorders(recorderNum, topicName)
		_ = initDeliverer(topicName)

		task := recordTask{Topic: topicName, Body: body, Header: header, Url: strUrl}
		recordTaskQueue <- task
		Println("record task added to record task queue")

		// inform user that we have got their request
		w.WriteHeader(http.StatusCreated)
		w.Write([]byte("success"))
	}
TAIL:
}

func listenForReplayRequest(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		io.WriteString(w, testReadForm)

	case "POST":

		/**
		 *If you make multipart/form-data POST request,
		 *ParseForm does not parse request body correctly (this might be a bug).
		 *So, use ParseMultipartForm if that is the case.
		**/
		//r.ParseForm()
		_ = r.ParseMultipartForm(10 * 1024 * 1024)

		frm := r.Form

		if _, ok := frm["topic"]; !ok {
			logWarning("topic name is missing when try to replay")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("BadRequest"))
			goto TAIL
		}
		topicNames, _ := frm["topic"]
		topicName := topicNames[0]
		Println("topic from test read request: ", topicName)

		if _, ok := frm["cmd"]; !ok {
			logWarning("command number is missing when try to replay")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("BadRequest"))
			goto TAIL
		}

		cmd, err := strconv.Atoi(frm["cmd"][0])
		Println("cmd from test read request: ", cmd)
		//logDebug("command number " + strconv.FormatInt(int64(cmd), 10))

		if nil != err {
			logWarning("convert command number from string to integer failed")
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte("BadRequest"))
			goto TAIL
		}

		if _, ok := topics[topicName]; ok {

			topic := topics[topicName]
			topic.deliverLog.WriteAt(Uint64ToBytes(uint64(cmd)), 0)
			topic.needRelodDelivery = true

			deliverer := initDeliverer(topicName)
			Println("deliverer inited")

			task := deliveryTask{Topic: topicName}
			deliverer.TaskChan <- task
			Println("deliverer task dispatched")

			// inform user that we have got their request
			w.WriteHeader(http.StatusCreated)
			w.Write([]byte("success"))
		} else {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("delivery service is not ready"))
		}
	}
TAIL:
}

func initRecorders(recorderNum int, topicName string) {
	if _, ok := recordTaskChanQueuePerTopic[topicName]; ok {
	} else {
		recordTaskChanQueue := make(chan chan recordTask, recorderNum)
		for i := 0; i < recorderNum; i++ {
			id := strconv.Itoa(i)
			id = topicName + id
			Println("creating recorder[%s]", id)
			recorder := newRecorder(id, recordTaskChanQueue)
			recorder.start()
		}
		recordTaskChanQueuePerTopic[topicName] = recordTaskChanQueue
		dispatchRecordTask(recordTaskChanQueue)
	}
}

func initDeliverer(topicName string) Deliverer {
	deliverer, ok := delivererPerTopic[topicName]
	if ok {
		Println("deliverer fetched from array")
	} else {
		deliverer = newDeliverer(topicName)
		deliverer.start()
		delivererPerTopic[topicName] = deliverer
		Println("new deliverer created")
	}
	return deliverer
}

func dispatchRecordTask(recordTaskChanQueue chan chan recordTask) {
	go func() {
		for {
			select {
			case task := <-recordTaskQueue:
				Printf("dispatcher pop 1 task[topic:%s,body:%s,header:%s,url:%s] from record task queue\n", task.Topic, task.Body, task.Header, task.Url)
				go func() {
					recordTaskChan := <-recordTaskChanQueue
					Println("select 1 recorderTaskChan from recordTaskChanQueue")
					Println("dispatch task to selected recorderTaskChan")
					recordTaskChan <- task
				}()
			}
		}
	}()
}
