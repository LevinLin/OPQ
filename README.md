# OPQ
* An **O**pen sourced **P**ersistent message **Q**ueue  
* Code is tested under go1.4.2 **(CAUTION: OPQ hasn't been tested in production environment so far)**
* **Features**  
  1.persistent message storage  
  2.push model - push message to target service and block when failure  
  3.easy to use - simple API whith HTTP POST method, no addtional client integration is required  
  4.message replay  
  5.high performance aimed  
  6.operations-friendly
* **Performance**  
  1.over 20,000(Message/Second) with 2K(Byte) message payload  
  2.over 30,000(Message/Second) with 1K(Byte) message payload
 
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

## Usage
**Push Message**  
| Tables        | Are           | Cool  |
| ------------- |:-------------:| -----:|
| col 3 is      | right-aligned | $1600 |
| col 2 is      | centered      |   $12 |
| zebra stripes | are neat      |    $1 |
|Url|Method|Parameter|
| :----: | :----: | :----:  |
|http://%{SERVER_NAME}[:%{SERVER_PORT}]/opq/push |||
**Replay Message**
  http://%{SERVER_NAME}[:%{SERVER_PORT}]/opq/replay
