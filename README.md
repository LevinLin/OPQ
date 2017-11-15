# OPQ
## About the design
### https://github.com/LevinLin/The-design-of-OPQ
* An **O**pen sourced **P**ersistent message **Q**ueue  
* Code is tested under go1.4.2 **(CAUTION: OPQ hasn't been tested in production environment so far)**
* **Features**  
  1.persistent message storage  
  2.push model - push message to target service and block when failure  
  3.easy to use - simple API whith HTTP POST method, no addtional client integration is required  
  4.message replay  
  5.high performance aimed  
  6.operations-friendly - graceful stop/restart, HA (TODO)
* **Performance**  
  1.over **20,000**(Message/Second) with 2K(Byte) message payload  
  2.over **30,000**(Message/Second) with 1K(Byte) message payload
  
  3.over **60,000**(Message/Second) with 128(Byte) message payload
 
## Install
### Download source code
```console
go get -u github.com/LevinLin/OPQ
```
### Build OPQ
```console
cd /path/to/OPQ
go build
```
### Run OPQ
```console
cd /path/to/OPQ
nohup ./OPQ &>/dev/null &
```
#### `-debug`
> System runs in debug model when given debug=yes, which will enable log/output in debug level, default to no
#### `-port`
> Listening port, default to 8999
#### `-syslog`
> System log name, default to system.log
#### ~`-admin`~
> Enable admin portal when given admin=yes, default to no **(TODO, not available yet)**
### Gracefully restart OPQ
```console
kill -1 %{PID}
```
## Usage
* **Push Message**  
  **url**: http://%{SERVER_NAME}[:%{SERVER_PORT}]/opq/push  
  **post fields**:  
  1.**url**: target url  
  2.**topic**: each message should belong to a topic    
  3.**message**: message content  
  **header**: specify the header if you need   
  **example**:(PHP)  
  ```php   
  <?php
    $url = "http://localhost:8999/opq/push";
    
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1);
    curl_setopt($ch, CURLOPT_POST, 1);

    $data = array(
        'url' => 'http://127.0.0.1/Comment/addComment?comment=nny&user=q18',
        'topic'=> 'comment',
        'message'=> 'this is message body',
    );
    curl_setopt($ch, CURLOPT_POSTFIELDS, $data);

    $response = curl_exec($ch);
    var_dump($response);
    curl_close($ch);
  ```
* **Replay Message**  
  **url**: http://%{SERVER_NAME}[:%{SERVER_PORT}]/opq/replay  
  **post fields**:  
  1.**topic**: topic name  
  2.**cmd**: commond number (message index, start from 0)       
  **example**:(PHP)  
  ```php   
  <?php
    $url = "http://localhost:8999/opq/replay";
    $ch = curl_init();
    curl_setopt($ch, CURLOPT_URL, $url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, 1); 
    curl_setopt($ch, CURLOPT_POST, 1); 
    
    $data = array(
        'topic'=> 'comment',
        'cmd'=> '30',
    );  
    curl_setopt($ch, CURLOPT_POSTFIELDS, $data);

    $response = curl_exec($ch);
    var_dump($response);
    curl_close($ch);
  ```
