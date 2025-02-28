---
sidebar_label: collectd
title: collectd writing
---

import CollectD from "../14-reference/_collectd.mdx"


collectd is a daemon used to collect system performance. collectd provides various storage mechanisms to store different values. It periodically counts system-related statistics while the system is running and storing information. You can use this information to help identify current system performance bottlenecks and predict future system load.

You can write the data collected by collectd to TDengine by simply pointing the configuration of collectd to the domain name (or IP address) and corresponding port of the server running taosAdapter. It can take full advantage of TDengine's efficient storage query performance and clustering capability for time-series data.

## Prerequisites

Writing collectd data to the TDengine requires several preparations.
- The TDengine cluster is deployed and running properly
- taosAdapter is installed and running, please refer to [taosAdapter's manual](/reference/taosadapter) for details
- collectd has been installed. Please refer to the [official documentation](https://collectd.org/download.shtml) to install collectd

## Configuration steps
<CollectD />

## Verification method

Restart collectd 

```
sudo systemctl restart collectd
```

Use the TDengine CLI to verify that data is written to TDengine from collectd and can read out correctly.

```
taos> show databases;
              name              |      created_time       |   ntables   |   vgroups   | replica | quorum |  days  |           keep           |  cache(MB)  |   blocks    |   minrows   |   maxrows   | wallevel |    fsync    | comp | cachelast | precision | update |   status   |
====================================================================================================================================================================================================================================================================================
 collectd                       | 2022-04-20 09:27:45.460 |          95 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ns        |      2 | ready      |
 log                            | 2022-04-20 07:19:50.260 |          11 |           1 |       1 |      1 |     10 | 3650                     |          16 |           6 |         100 |        4096 |        1 |        3000 |    2 |         0 | ms        |      0 | ready      |
Query OK, 2 row(s) in set (0.003266s)

taos> use collectd;
Database changed.

taos> show stables;
              name              |      created_time       | columns |  tags  |   tables    |
============================================================================================
 load_1                         | 2022-04-20 09:27:45.492 |       2 |      2 |           1 |
 memory_value                   | 2022-04-20 09:27:45.463 |       2 |      3 |           6 |
 df_value                       | 2022-04-20 09:27:45.463 |       2 |      4 |          25 |
 load_2                         | 2022-04-20 09:27:45.501 |       2 |      2 |           1 |
 load_0                         | 2022-04-20 09:27:45.485 |       2 |      2 |           1 |
 interface_1                    | 2022-04-20 09:27:45.488 |       2 |      3 |          12 |
 irq_value                      | 2022-04-20 09:27:45.476 |       2 |      3 |          31 |
 interface_0                    | 2022-04-20 09:27:45.480 |       2 |      3 |          12 |
 entropy_value                  | 2022-04-20 09:27:45.473 |       2 |      2 |           1 |
 swap_value                     | 2022-04-20 09:27:45.477 |       2 |      3 |           5 |
Query OK, 10 row(s) in set (0.002236s)

taos> select * from collectd.memory_value limit 10;
              ts               |           value           |              host              |         type_instance          |           type           |
=========================================================================================================================================================
 2022-04-20 09:27:45.459653462 |        54689792.000000000 | shuduo-1804                    | buffered                       | memory                   |
 2022-04-20 09:27:55.453168283 |        57212928.000000000 | shuduo-1804                    | buffered                       | memory                   |
 2022-04-20 09:28:05.453004291 |        57942016.000000000 | shuduo-1804                    | buffered                       | memory                   |
 2022-04-20 09:27:45.459653462 |      6381330432.000000000 | shuduo-1804                    | free                           | memory                   |
 2022-04-20 09:27:55.453168283 |      6357643264.000000000 | shuduo-1804                    | free                           | memory                   |
 2022-04-20 09:28:05.453004291 |      6349987840.000000000 | shuduo-1804                    | free                           | memory                   |
 2022-04-20 09:27:45.459653462 |       107040768.000000000 | shuduo-1804                    | slab_recl                      | memory                   |
 2022-04-20 09:27:55.453168283 |       107536384.000000000 | shuduo-1804                    | slab_recl                      | memory                   |
 2022-04-20 09:28:05.453004291 |       107634688.000000000 | shuduo-1804                    | slab_recl                      | memory                   |
 2022-04-20 09:27:45.459653462 |       309137408.000000000 | shuduo-1804                    | used                           | memory                   |
Query OK, 10 row(s) in set (0.010348s)
```

