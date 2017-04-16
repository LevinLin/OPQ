# OPQ
* An Open sourced Persistent message Queue  
* Code is tested under go1.4.2 and go1.7.3
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
```console
System runs in debug model when given debug=yes, which will enable log/output in debug level, default to no
```
#### `-port`
```console
Listening port, default to 8999
```
#### `-syslog`
```console
System log name, default to system.log
```
#### `-admin` 
```console
Enable admin portal when given admin=yes, default to no *(TODO, not available yet)*
```

