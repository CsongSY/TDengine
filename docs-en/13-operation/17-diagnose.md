---
sidebar_label: Diagnose
title: Diagnose Problems
---

## Diagnose Network Connection

When the client is unable to access the server, the network connection between the client side and the server side needs to be checked to find out the root cause and resolve problems.

The diagnostic for network connection can be executed between Linux and Linux or between Linux and Windows.

Diagnostic steps：

1. If the port range to be diagnosed are being occupied by a `taosd` server process, please firstly stop `taosd.
2. On the server side, execute command `taos -n server -P <port> -l <pktlen>` to monitor the port range starting from the port specified by `-P` parameter with the role of "server.
3. On the client side, execute command `taos -n client -h <fqdn of server> -P <port> -l <pktlen>` to send testing package to the specified server and port.
 
-l <pktlen\>： The size of the testing package, in bytes. The value range is [11, 64,000] and default value is 1,000. Please be noted that the package length must be same in the above 2 commands executed on server side and client side respectively.

Output of the server side is as below for example:

```bash
# taos -n server -P 6000
12/21 14:50:13.522509 0x7f536f455200 UTL work as server, host:172.27.0.7 startPort:6000 endPort:6011 pkgLen:1000

12/21 14:50:13.522659 0x7f5352242700 UTL TCP server at port:6000 is listening
12/21 14:50:13.522727 0x7f5351240700 UTL TCP server at port:6001 is listening
...
...
...
12/21 14:50:13.523954 0x7f5342fed700 UTL TCP server at port:6011 is listening
12/21 14:50:13.523989 0x7f53437ee700 UTL UDP server at port:6010 is listening
12/21 14:50:13.524019 0x7f53427ec700 UTL UDP server at port:6011 is listening
12/21 14:50:22.192849 0x7f5352242700 UTL TCP: read:1000 bytes from 172.27.0.8 at 6000
12/21 14:50:22.192993 0x7f5352242700 UTL TCP: write:1000 bytes to 172.27.0.8 at 6000
12/21 14:50:22.237082 0x7f5351a41700 UTL UDP: recv:1000 bytes from 172.27.0.8 at 6000
12/21 14:50:22.237203 0x7f5351a41700 UTL UDP: send:1000 bytes to 172.27.0.8 at 6000
12/21 14:50:22.237450 0x7f5351240700 UTL TCP: read:1000 bytes from 172.27.0.8 at 6001
12/21 14:50:22.237576 0x7f5351240700 UTL TCP: write:1000 bytes to 172.27.0.8 at 6001
12/21 14:50:22.281038 0x7f5350a3f700 UTL UDP: recv:1000 bytes from 172.27.0.8 at 6001
12/21 14:50:22.281141 0x7f5350a3f700 UTL UDP: send:1000 bytes to 172.27.0.8 at 6001
...
...
...
12/21 14:50:22.677443 0x7f5342fed700 UTL TCP: read:1000 bytes from 172.27.0.8 at 6011
12/21 14:50:22.677576 0x7f5342fed700 UTL TCP: write:1000 bytes to 172.27.0.8 at 6011
12/21 14:50:22.721144 0x7f53427ec700 UTL UDP: recv:1000 bytes from 172.27.0.8 at 6011
12/21 14:50:22.721261 0x7f53427ec700 UTL UDP: send:1000 bytes to 172.27.0.8 at 6011
```

Output of the client side is as below for example:

```bash
# taos -n client -h 172.27.0.7 -P 6000
12/21 14:50:22.192434 0x7fc95d859200 UTL work as client, host:172.27.0.7 startPort:6000 endPort:6011 pkgLen:1000

12/21 14:50:22.192472 0x7fc95d859200 UTL server ip:172.27.0.7 is resolved from host:172.27.0.7
12/21 14:50:22.236869 0x7fc95d859200 UTL successed to test TCP port:6000
12/21 14:50:22.237215 0x7fc95d859200 UTL successed to test UDP port:6000
...
...
...
12/21 14:50:22.676891 0x7fc95d859200 UTL successed to test TCP port:6010
12/21 14:50:22.677240 0x7fc95d859200 UTL successed to test UDP port:6010
12/21 14:50:22.720893 0x7fc95d859200 UTL successed to test TCP port:6011
12/21 14:50:22.721274 0x7fc95d859200 UTL successed to test UDP port:6011
```

The output needs to be checked carefully for the system operator to find out root cause and solve the problem.

## Startup Status and RPC Diagnostic

`taos -n startup -h <fqdn of server>` can be used to check the startup status of a `taosd` process. This is a comman task for a system operator to do to determine whether `taosd` has been started successfully, especially in case of cluster.

`taos -n rpc -h <fqdn of server>` can be used to check whether the port of a started `taosd` can be accessed or not. If `taosd` process doesn't respond or work abnormally, this command can be used to initiate a rpc communication with the specified fqdn to determine whether it's network problem or `taosd` is abnormal.

## Sync and Arbitrator Diagnostic

```bash
taos -n sync -P 6040 -h <fqdn of server>
taos -n sync -P 6042 -h <fqdn of server>
```

The above commands can be executed on Linux Shell to check whether the port for sync works well and whether the sync module of the server side works well. Besides, `-P 6042` is used to check whether the arbitrator is configured properly and works well.

## Network Speed Diagnostic

`taos -n speed -h <fqdn of server> -P 6030 -N 10 -l 10000000 -S TCP`

From version 2.2.0.0, the above command can be executed on Linux Shell to test the network speed, it sends uncompressed package to a running `taosd` server process or a simulated server process started by `taos -n server` to test the network speed. Parameters can be used when testing network speed are as below:

-n：When set to "speed", it means testing network speed
-h：The FQDN or IP of the server process to be connected to; if not set, the FQDN configured in `taos.cfg` is used
-P：The port of the server process to connect to, the default value is 6030
-N：The number of packages that will be sent in the test, range is [1,10000], default value is 100
-l：The size of each package in bytes, range is [1024, 1024 \* 1024 \* 1024], default value is 1024
-S：The type of network packages to send, can be either TCP or UDP, default value is 

## FQDN Resolution Diagnostic

`taos -n fqdn -h <fqdn of server>`

From version 2.2.0.0, the above command can be executed on Linux Shell to test the resolution speed of FQDN. It can be used to try to resolve a FQDN to an IP address and record the time spent in this process. The parameters that can be used for this purpose are as below:

-n：When set to "fqdn", it means testing the speed of resolving FQDN
-h：The FQDN to be resolved. If not set, the `FQDN` parameter in `taos.cfg` is used by default.

## Server Log

The parameter `debugFlag` is used to control the log level of `taosd` server process. The default value is 131, for debug purpose it needs to be escalated to 135 or 143. 

Once this parameter is set to 135 or 143, the log file grows very quickly especially when there is huge volume of data insertion and data query requests. If all the logs are stored together, some important information may be missed very easily, so on server side important information is stored at different place from other logs.一

- The log at level of INFO, WARNING and ERROR is stored in `taosinfo` so that it is easy to find important information 
- The log at level of DEBUG (135) and TRACE (143) and other information not handled by `taosinfo` are stored in `taosdlog`

## Client Log

An independent log file, named as "taoslog+<seq num\>" is generated for each client program, i.e. a client process. The default value of `debugfalg` is also 131 and only log at level of INFO/ERROR/WARNING is recorded, it and needs to be changed to 135 or 143 so that log at DEBUG or TRACE level can be recorded for debugging purpose.

The maximum length of a single log file is controlled by parameter `numOfLogLines` and only 2 log files are kept for each `taosd` server process.

log file is written in async way to minimize the workload on disk, bu the penalty is that a few log lines may be lost in some extreme conditions. 
