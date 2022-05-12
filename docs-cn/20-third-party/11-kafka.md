---
sidebar_label: Kafka
title: TDengine Kafka Connector 使用教程
---

TDengine Kafka Connector 本质上是 Kafka Connect 的两个插件: TDengine Source Connector 和 TDengine Sink Connector。借助 Confluent 平台，用户只需提供简单的配置文件，就可以将 Kafka 中指定 topic 的数据同步到 TDengine， 或将 TDengine 中指定数据库的数据同步到 Kafka。

## 什么是 Kafka Connect？

如今 Apache Kafka 已经从最初的消息队列发展成为一个完备的事件流平台。Kafka Connect 就是这个平台的一个组件，用于使其它系统， 比如数据库、云服务、文件系统等能方便地连接到 Kafka。数据既可以通过 Kafka Connect 从其它系统流向 Kafka, 也可以通过 Kafka Connect 从 Kafka 流向其它系统。

![](Kafka_Connect.png)

Kafka Connect 可以看作由众多插件组成的生态系统。从其它系统读数据的插件称为 Source Connector, 写数据到其它系统的插件称为 Sink Connector。TDengine Source Connector 的作用就是把数据实时地从 TDengine 读出来交给 Kafka Connect。TDengine Sink Connector 的作用就是 从 Kafka Connect 接收数据写入 TDengine 的插件。Source Connector 和 Sink Connector 都不会直接连接 Kafka Broker。 Source Connector 只是把数据转交给 Kafka Connect。 SinkConnector 也只需从 Kafka Connect 接收数据。所以 Kafka Connect 也可以看作 Kafka 的一个客户端应用。Kafka Connect 有自己独立的进程，自身的设计也是分布式的。

![](streaming-integration-with-kafka-connect.png)

Kafka 已经提供了生产者和消费者 API 以及客户端库用来与 Kafka 集成，为什么还需要 Kafka Connect？ 因为一个好 Kafka 客户端程序，不是单单生产或消费数据，还需要考虑容错、重启、日志、弹性伸缩、序列化、反序列化等。当开发者自己完成了这一切就开发了一个和 Kafka Connect 类似的东西。与 Kafka 集成是 Kafka Connect 已经解决的问题, 用户不需要重复造轮子，只有在少数边缘场景才需要定制化的集成方案。

## 什么是 Confluent？

Confluent 在 Kafka 的基础上增加很多扩展功能。比如：

1. Schema Registry
2. REST 代理
3. 非 Java 客户端
4. 很多打包好的 Kafka Connect 插件
5. 管理和监控 Kafka 的 GUI —— Confluent 控制中心

这些扩展功能有的包含在社区版本的 Confluent 中，有的只有企业版能用。
![](confluentPlatform.png)

Confluent 企业版提供了 `confluent` 命令行工具管理各个组件。在本教程我们将多次用到此命令。

## 前置条件

运行本教程中示例的前提条件。

1. Linux 操作系统
2. 已安装 Java 8 和 Maven
3. 已安装 Git
4. 已安装并启动 TDengine。如果还没有可参考[安装和卸载](/operation/pkg-install)

## 安装 Confluent

Confluent 提供了 Docker 和二进制包两种安装方式。本教程为了演示简单（无需进入容器操作），采用二进制包的方式安装。在任意目录下执行：

```
curl -O http://packages.confluent.io/archive/7.1/confluent-7.1.1.tar.gz
tar xzf confluent-7.1.1.tar.gz
mv confluent-7.1.1 /opt
```

然后需要把 `$CONFLUENT_HOME/bin` 目录加入 PATH。

```title=".profile"
export CONFLUENT_HOME=/opt/confluent-7.1.1
PATH=$CONFLUENT_HOME/bin
export PATH
```

以上脚本可以追加到当前用户的 profile 文件（~/.profile 或 ~/.bash_profile）

安装完成之后，可以输入`confluent version`做简单验证：

```
# confluent version
confluent - Confluent CLI

Version:     v2.6.1
Git Ref:     6d920590
Build Date:  2022-02-18T06:14:21Z
Go Version:  go1.17.6 (linux/amd64)
Development: false
```

## 安装 TDengine Connector 插件

### 从源码安装

```
git@github.com:taosdata/kafka-connect-tdengine.git
kafka-connect-tdengine
mvn clean package
cd target/components/packages/
cp taosdata-kafka-connect-tdengine-0.1.0.zip  $CONFLUENT_HOME/share/
confluent-hub-components/
cd $CONFLUENT_HOME/share/confluent-hub-components/
unzip taosdata-kafka-connect-tdengine-0.1.0.zip
rm taosdata-kafka-connect-tdengine-0.1.0.zip
```

以上脚本先 clone 项目源码，然后用 maven 编译打包。打包完成后在 `target/components/packages/` 目录生成了插件的 zip 包。然后把这个 zip 包解压到安装插件的路径即可。安装插件的路径在配置文件 `$CONFLUENT_HOME/etc/kafka/connect-standalone.properties` 中配置，以上命令使用了默认的插件路径 `$CONFLUENT_HOME/share/confluent-hub-components/`。

执行完上面的操作，TDengine Sink Connector 和 TDengine Source Connector 都已安装。

### 用 confluent-hub 安装

[Confluent Hub](https://www.confluent.io/hub) 提供下载 Kafka Connect 插件的服务。可以用命令工具 `confluent-hub` 安装已发布到 Confluent Hub 的插件。
**TDengine Kafka Connector 目前没有正式发布，不能用这种方式安装**。

## 启动 Confluent

```
confluent local services start
```

:::note
一定要先安装插件再启动 Confluent, 否则会出现找不到类的错误。在启动过程中可以监控 Kafka Connect 的日志（默认路径: /tmp/confluent.xxxx/connect/logs/connect.log），日志中会输出发现的插件，可以用这个方法验证插件是否安装成功。
:::

:::tip
如果某个组件启动失败，可尝试清空数据，重新启动。数据目录在启动时会打印到控制台，比如 ：

```title="控制台输出日志" {1}
Using CONFLUENT_CURRENT: /tmp/confluent.106668
Starting ZooKeeper
ZooKeeper is [UP]
Starting Kafka
Kafka is [UP]
Starting Schema Registry
Schema Registry is [UP]
Starting Kafka REST
Kafka REST is [UP]
Starting Connect
Connect is [UP]
Starting ksqlDB Server
ksqlDB Server is [UP]
Starting Control Center
Control Center is [UP]
```

可执行 `rm -rf /tmp/confluent.106668` 清空数据。
:::

## Sink Connector 的使用

Sink Connector 的作用是同步某些 topic 的数据到 TDengine。topic 的名字对应 TDengine 中超级表的名字。用户无需提前创建数据库和超级表。目标数据库的名字可以手动指定（见配置参数 connection.database）， 也可以按一定规则生成(见配置参数 connection.database.prefix)。

TDengine Sink Connector 内部使用 TDengine [无模式写入接口](/reference/connector/cpp#无模式写入-api)写数据到 TDengine，目前支持三种格式的数据：[InfluxDB 行协议格式](/develop/insert-data/influxdb-line)、 [OpenTSDB 行协议格式](/develop/insert-data/opentsdb-telnet) 和 [OpenTSDB JSON 协议格式](/develop/insert-data/opentsdb-json)。

下面的示例将主题 meters 的数据，同步到目标数据库 power。数据格式为 InfluxDB Line 协议格式。

### 添加配置文件

```
mkdir ~/test
cd ~/test
vi sink-demo.properties
```

sink-demo.properties 内容如下：

```ini title="sink-demo.properties"
name=tdengine-sink-demo
connector.class=com.taosdata.kafka.connect.sink.TDengineSinkConnector
tasks.max=1
topics=meters
connection.url=jdbc:TAOS://127.0.0.1:6030
connection.user=root
connection.password=taosdata
connection.database=power
db.schemaless=line
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
```

关键配置说明：

1. `topics=meters` 和 `connection.database=power`, 表示订阅主题 meters 的数据，并写入数据库 power。
2. `db.schemaless=line`, 表示使用 InfluxDB Line 协议格式的数据。

### 加载 Connector 插件

```
confluent local services connect connector load TDengineSinkConnector --config ./sink-demo.properties
```

如果以上命令执行成功，则会有如下输出：

```json
{
  "name": "TDengineSinkConnector",
  "config": {
    "connection.database": "power",
    "connection.password": "taosdata",
    "connection.url": "jdbc:TAOS://127.0.0.1:6030",
    "connection.user": "root",
    "connector.class": "com.taosdata.kafka.connect.sink.TDengineSinkConnector",
    "db.schemaless": "line",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "tasks.max": "1",
    "topics": "meters",
    "value.converter": "org.apache.kafka.connect.storage.StringConverter",
    "name": "TDengineSinkConnector"
  },
  "tasks": [],
  "type": "sink"
}
```

### 写入测试数据

使用 kafka-console-producer 向主题 meters 添加测试数据。

```
[root@host user]# kafka-console-producer --broker-list localhost:9092 --topic meters
>meters,location=Beijing.Haidian,groupid=2 current=11.8,voltage=221,phase=0.28 1648432611249000000
>meters,location=Beijing.Haidian,groupid=2 current=13.4,voltage=223,phase=0.29 1648432611250000000
>meters,location=Beijing.Haidian,groupid=3 current=10.8,voltage=223,phase=0.29 1648432611249000000
>meters,location=Beijing.Haidian,groupid=3 current=11.3,voltage=221,phase=0.35 1648432611250000000
```

:::note
如果目标数据库 power 不存在，TDengine Sink Connector 会自动创建数据库。自动创建数据库使用的时间精度为纳秒，这就要求写入数据的时间戳精度也是纳秒。如果写入数据的时间戳精度不是纳秒，将会抛异常。
:::

### 验证同步是否成功

使用 TDengine CLI 验证同步是否成功。

```
taos> use power;
Database changed.

taos> select * from meters;
              ts               |          current          |          voltage          |           phase           | groupid |            location            |
===============================================================================================================================================================
 2022-03-28 09:56:51.249000000 |              11.800000000 |             221.000000000 |               0.280000000 | 2       | Beijing.Haidian                |
 2022-03-28 09:56:51.250000000 |              13.400000000 |             223.000000000 |               0.290000000 | 2       | Beijing.Haidian                |
 2022-03-28 09:56:51.249000000 |              10.800000000 |             223.000000000 |               0.290000000 | 3       | Beijing.Haidian                |
 2022-03-28 09:56:51.250000000 |              11.300000000 |             221.000000000 |               0.350000000 | 3       | Beijing.Haidian                |
Query OK, 4 row(s) in set (0.004208s)
```

如果看到以上数据，则同步成功。如果没有请检查 Kafka Connect 的日志。配置参数的详细说明见[配置参考](#配置参考)。


## Source Connector 的使用

### 添加配置文件

```
vi source-demo.properties
```

输入以下内容：

```ini tittle="source-demo.properties"
name=tdengine-source
connector.class=com.taosdata.kafka.connect.source.TDengineSourceConnector
tasks.max=1
connection.url=jdbc:TAOS://127.0.0.1:6030
connection.username=root
connection.password=taosdata
connection.database=test
connection.attempts=3
connection.backoff.ms=5000
topic.prefix=tdengine-
poll.interval.ms=1000
fetch.max.rows=100
out.format=json
key.converter=org.apache.kafka.connect.storage.StringConverter
value.converter=org.apache.kafka.connect.storage.StringConverter
```

## 配置参考

### 通用配置

以下配置项对 TDengine Sink Connector 和 TDengine Source Connector 均适用。

1. `connector.class` : connector 的完整类名， 如: com.taosdata.kafka.connect.sink.TDengineSinkConnector
2. `tasks.max` : 最大任务数, 默认为 1
3. `topics` : 需要订阅的 topic 列表， 多个用逗号分隔, 如 `topic1,topic2`。
4. `connection.url` : TDengine JDBC 连接字符串， 如 `jdbc:TAOS://127.0.0.1:6030`
5. `connection.user` ： TDengine 用户名， 默认 root
6. `connection.password` ：TDengine 用户密码， 默认 taosdata
7. `connection.database` ： 目标数据库名。如果指定的数据库不存在会则自动创建。自动建库使用的时间精度为纳秒。默认值为 null。为 null 时目标数据库命名规则参考 `connection.database.prefix` 参数的说明
8. `connection.attempts` ：最大尝试连接次数。默认为 3。
9. `connection.backoff.ms` ： Backoff time in milliseconds between connection attempts.

### Sink Connector 特有的配置

1. `connection.database.prefix` ： when connection.database is not specify, a string for the destination database name. which may contain '${topic}' as a placeholder for the originating topic name for example, kafka_${topic} for the topic 'orders' will map to the database name 'kafka_orders'. the default value is null, this means the topic will be mapped to the new database which will have same name as the topic
2. `batch.size` : Specifies how many records to attempt to batch together for insertion into the destination table
3. `max.retries`: The maximum number of times retry on errors before falling the task.
4. `retry.backoff.ms` : The time in milliseconds to wait following an error before a retry attempt is made.
5. `db.schemaless` : the format to write data to tdengine, one of line,telnet,json

### Source Connector 特有的配置

1. `topic.prefix`： topic 前缀，完整的 topic 名称为： `topic.prefix` + `connection.database`

## 参考

1. https://www.confluent.io/what-is-apache-kafka
2. https://developer.confluent.io/learn-kafka/kafka-connect/intro
3. https://docs.confluent.io/platform/current/platform.html
