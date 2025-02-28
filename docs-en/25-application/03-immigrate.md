---
sidebar_label: OpenTSDB Migration to TDengine
title: Best Practices for Migrating OpenTSDB Applications to TDengine
---

As a distributed, scalable, HBase-based distributed time-series database software, thanks to its first-mover advantage, OpenTSDB has been introduced and widely used in DevOps by people. However, using new technologies like cloud computing, microservices, and containerization technology with rapid development. Enterprise-level services are becoming more and more diverse. The architecture is becoming more complex.

From this situation, it increasingly plagues to use of OpenTSDB as a DevOps backend storage for monitoring by performance issues and delayed feature upgrades. The resulting increase in application deployment costs and reduced operational efficiency.
These problems are becoming increasingly severe as the system scales up.

To meet the fast-growing IoT big data market and technical needs, TAOSData developed an innovative big-data processing product, **TDengine**.

After learning the advantages of many traditional relational databases and NoSQL databases, stream computing engines, and message queues, TDengine has its unique benefits in time-series big data processing. TDengine can effectively solve the problems currently encountered by OpenTSDB.

Compared with OpenTSDB, TDengine has the following distinctive features.

- Performance of data writing and querying far exceeds that of OpenTSDB.
- Efficient compression mechanism for time-series data, which compresses less than 1/5 of the storage space on disk.
- The installation and deployment are straightforward. A single installation package can complete the installation and deployment and does not rely on other third-party software. The entire installation and deployment process in a few seconds;
- The built-in functions cover all of OpenTSDB's query functions. And support more time-series data query functions, scalar functions, and aggregation functions. And support advanced query functions such as multiple time-window aggregations, join query, expression operation, multiple group aggregation, user-defined sorting, and user-defined functions. Adopting SQL-like syntax rules is more straightforward and has no learning cost.
- Supports up to 128 tags, with a total tag length of 16 KB.
- In addition to the REST interface, it also provides interfaces to Java, Python, C, Rust, Go, C# and other languages. Its supports a variety of enterprise-class standard connector protocols such as JDBC.

If we migrate the applications originally running on OpenTSDB to TDengine, we will effectively reduce the compute and storage resource consumption and the number of deployed servers. And will also significantly reduce the operation and maintenance costs, making operation and maintenance management more straightforward and more accessible, and considerably reducing the total cost of ownership. Like OpenTSDB, TDengine has also been open-sourced, including the stand-alone version and the cluster version source code. So there is no need to be concerned about the vendor-lock problem.

We will explain how to migrate OpenTSDB applications to TDengine quickly, securely, and reliably without coding, using the most typical DevOps scenarios. Subsequent chapters will go into more depth to facilitate migration for non-DevOps systems.

## DevOps Application Quick Migration

### 1. Typical Application Scenarios

The following figure (Figure 1) shows the system's overall architecture for a typical DevOps application scenario.

**Figure 1. Typical architecture in a DevOps scenario**
Figure 1. [IT-DevOps-Solutions-Immigrate-OpenTSDB-Arch](/img/IT-DevOps-Solutions-Immigrate-OpenTSDB-Arch.jpg "Figure 1. Typical architecture in a DevOps scenario")

In this application scenario, there are Agent tools deployed in the application environment to collect machine metrics, network metrics, and application metrics. Data collectors to aggregate information collected by agents, systems for persistent data storage and management, and tools for monitoring data visualization (e.g., Grafana, etc.).

The agents deployed in the application nodes are responsible for providing operational metrics from different sources to collectd/Statsd. And collectd/StatsD is accountable for pushing the aggregated data to the OpenTSDB cluster system and then visualizing the data using the visualization kanban board software, Grafana.

### 2. Migration Services

- **TDengine installation and deployment**

First of all, please install TDengine. Download the latest stable version of TDengine from the official website and install it. For help with using various installation packages, please refer to the blog ["Installation and Uninstallation of TDengine Multiple Installation Packages"](https://www.taosdata.com/blog/2019/08/09/566.html).

Note that once the installation is complete, do not start the `taosd` service immediately, but after properly configuring the parameters.

- **Adjusting the data collector configuration**

TDengine version 2.4 and later version includes `taosAdapter`. taosAdapter is a stateless, rapidly elastic, and scalable component. taosAdapter supports Influxdb's Line Protocol and OpenTSDB's telnet/JSON writing protocol specification, providing rich data access capabilities, effectively saving user migration costs and reducing the difficulty of user migration.

Users can flexibly deploy taosAdapter instances according to their requirements to rapidly improve the throughput of data writes in conjunction with the needs of scenarios and provide guarantees for data writes in different application scenarios.

Through taosAdapter, users can directly push the data collected by `collectd` or `StatsD` to TDengine to achieve seamless migration of application scenarios, which is very easy and convenient. taosAdapter also supports Telegraf, Icinga, TCollector, and node_exporter data. For more details, please refer to [taosAdapter](/reference/taosadapter/).

If using collectd, modify the configuration file in its default location `/etc/collectd/collectd.conf` to point to the IP address and port of the node where to deploy taosAdapter. For example, assuming the taosAdapter IP address is 192.168.1.130 and port 6046, configure it as follows.

```html
LoadPlugin write_tsdb
<Plugin write_tsdb>
  <Node>
    Host "192.168.1.130" Port "6046" HostTags "status=production" StoreRates
    false AlwaysAppendDS false
  </Node>
</Plugin>
```

You can use collectd and push the data to taosAdapter utilizing the push to OpenTSDB plugin. taosAdapter will call the API to write the data to TDengine, thus completing the writing of the data. If you are using StatsD, adjust the profile information accordingly.

- **Tuning the Dashboard system**

After writing the data to TDengine properly, you can adapt Grafana to visualize the data written to TDengine. To obtain and use the Grafana plugin provided by TDengine, please refer to [Links to other tools](/third-party/grafana).

TDengine provides two sets of Dashboard templates by default, and users only need to import the templates from the Grafana directory into Grafana to activate their use.

**Importing Grafana Templates** Figure 2.
! [](/img/IT-DevOps-Solutions-Immigrate-OpenTSDB-Dashboard.jpg "Figure 2. Importing a Grafana Template")

After the above steps, you completed the migration to replace OpenTSDB with TDengine. You can see that the whole process is straightforward, there is no need to write any code, and only some configuration files need to be adjusted to meet the migration work.

### 3. Post-migration architecture

After completing the migration, the figure below (Figure 3) shows the system's overall architecture. The whole process of the acquisition side, the data writing, and the monitoring and presentation side are all kept stable, except for a few configuration adjustments, which do not involve any critical changes or alterations. OpenTSDB to TDengine migration action, using TDengine more powerful processing power and query performance.

In most DevOps scenarios, if you have a small OpenTSDB cluster (3 or fewer nodes) for providing the storage layer of DevOps and rely on OpenTSDB to give a data persistence layer and query capabilities,  you can safely replace OpenTSDB with TDengine. TDengine will save more compute and storage resources. With the same compute resource allocation, a single TDengine can meet the service capacity provided by 3 to 5 OpenTSDB nodes. If the scale is more prominent, then TDengine clustering is required.

Suppose your application is particularly complex, or the application domain is not a DevOps scenario. You can continue reading subsequent chapters for a more comprehensive and in-depth look at the advanced topics of migrating an OpenTSDB application to TDengine.

**Figure 3. System architecture after migration**
! [IT-DevOps-Solutions-Immigrate-TDengine-Arch](/img/IT-DevOps-Solutions-Immigrate-TDengine-Arch.jpg "Figure 3. System architecture after migration completion")

## Migration evaluation and strategy for other scenarios

### 1. Differences between TDengine and OpenTSDB

This chapter describes the differences between OpenTSDB and TDengine at the system functionality level. After reading this chapter, you can fully evaluate whether you can migrate some complex OpenTSDB-based applications to TDengine, and what you should pay attention to after migration.

TDengine currently only supports Grafana for visual kanban rendering, so if your application uses front-end kanban boards other than Grafana (e.g., [TSDash](https://github.com/facebook/tsdash), [Status Wolf](https://github) .com/box/StatusWolf), etc.). You cannot directly migrate those front-end kanbans to TDengine, and the front-end kanban will need to be ported to Grafana to work correctly.

TDengine version 2.3.0.x only supports collectd and StatsD as data collection aggregation software but will provide more data collection aggregation software in the future. If you use other data aggregators on the collection side, your application needs to be ported to these two data aggregation systems to write data correctly.
In addition to the two data aggregator software protocols mentioned above, TDengine also supports writing data directly via InfluxDB's row protocol and OpenTSDB's data writing protocol, JSON format. You can rewrite the logic on the data push side to write data using the row protocols supported by TDengine.

In addition, if your application uses the following features of OpenTSDB, you need to understand the following considerations before migrating your application to TDengine.

1. `/api/stats`: If your application uses this feature to monitor the service status of OpenTSDB, and you have built the relevant logic to link the processing in your application, then this part of the status reading and fetching logic needs to be re-adapted to TDengine. TDengine provides a new mechanism for handling cluster state monitoring to meet the monitoring and maintenance needs of your application.
2. `/api/tree`: If you rely on this feature of OpenTSDB for the hierarchical organization and maintenance of timelines, you cannot migrate it directly to TDengine, which uses a database -> super table -> sub-table hierarchy to organize and maintain timelines, with all timelines belonging to the same super table in the same system hierarchy, but it is possible to simulate a logical multi-level structure of the application through the unique construction of different tag values.
3. `Rollup And PreAggregates`: The use of Rollup and PreAggregates requires the application to decide where to access the Rollup results and, in some scenarios, to access the actual results. The opacity of this structure makes the application processing logic extraordinarily complex and not portable at all. We think this strategy is a compromise when the time-series database does not.
TDengine does not support automatic downsampling of multiple timelines and preaggregation (for a range of periods) for the time being. Still, thanks to its high-performance query processing logic can provide very high-performance query responses without relying on Rollup and preaggregation (for a range of periods), making your application query processing logic much more straightforward.
The logic is much simpler.
4. `Rate`: TDengine provides two functions to calculate the rate of change of values, namely `Derivative` (the result is consistent with the Derivative behavior of InfluxDB) and `IRate` (the result is compatible with the IRate function in Prometheus). However, the results of these two functions are slightly different from Rate, but the functions are more powerful overall. In addition, TDengine supports all the calculation functions provided by OpenTSDB, and TDengine's query functions are much more potent than those supported by OpenTSDB, which can significantly simplify the processing logic of your application.

Through the above introduction, I believe you should be able to understand the changes brought about by the migration of OpenTSDB to TDengine. And this information will also help you correctly determine whether you would migrate your application to TDengine to experience the powerful and convenient time-series data processing capability provided by TDengine.

### 2. Migration strategy suggestion

First, the OpenTSDB-based system migration involves data schema design, system scale estimation, and data write end transformation, data streaming, and application adaptation; after that, the two systems will run in parallel for a while and then migrate the historical data to TDengine. Of course, if your application has some functions that strongly depend on the above OpenTSDB features and you do not want to stop using them, you can migrate the historical data to TDengine.
You can consider keeping the original OpenTSDB system running while starting TDengine to provide the primary services.

## Data model design

On the one hand, TDengine requires a strict schema definition for its incoming data. On the other hand, the data model of TDengine is richer than that of OpenTSDB, and the multi-valued model is compatible with all single-valued model building requirements.

Let us now assume a DevOps scenario where we use collectd to collect the underlying metrics of the device, including memory, swap, disk, etc. The schema in OpenTSDB is as follows.

| metric | value name | type | tag1 | tag2 | tag3 | tag4 | tag5 |
| ---- | -------------- | ------ | ------ | ---- | ----------- | -------------------- | --------- | ------ |
| 1 | memory | value | double | host | memory_type | memory_type_instance | source | n/a |
| 2 | swap | value | double | host | swap_type | swap_type_instance | source | n/a |
| 3 | disk | value | double | host | disk_point | disk_instance | disk_type | source |

TDengine requires the data stored to have a data schema, i.e., you need to create a super table and specify the schema of the super table before writing the data. For data schema creation, you have two ways to do this: 1) Take advantage of TDengine's native data writing support for OpenTSDB by calling the TDengine API to write (text line or JSON format)
and automate the creation of single-value models. This approach does not require significant adjustments to the data writing application, nor does it require converting the written data format.

At the C level, TDengine provides the `taos_schemaless_insert()` function to write data in OpenTSDB format directly (in early version this function was named `taos_insert_lines()`). Please refer to the sample code `schemaless.c` in the installation package directory as reference.

(2) based on a complete understanding of TDengine's data model,  to establish the mapping relationship between OpenTSDB and TDengine's data model adjustment manually. Considering that OpenTSDB is a single-value mapping model, recommended using the single-value model in TDengine. TDengine can support both multi-value and single-value models.

- **Single-valued model**.

The steps are as follows: use the name of the metrics as the name of the TDengine super table, which build with two basic data columns - timestamp and value, and the label of the super table is equivalent to the label information of the metrics, and the number of labels is equal to the number of labels of the metrics. The names of sub-tables are named with fixed rules: `metric + '_' + tags1_value + '_' + tag2_value + '_' + tag3_value ...` as the sub-table name.

Create 3 super tables in TDengine.

```sql
create stable memory(ts timestamp, val float) tags(host binary(12), memory_type binary(20), memory_type_instance binary(20), source binary(20)) ;
create stable swap(ts timestamp, val double) tags(host binary(12), swap_type binary(20), swap_type_binary binary(20), source binary(20));
create stable disk(ts timestamp, val double) tags(host binary(12), disk_point binary(20), disk_instance binary(20), disk_type binary(20), source binary(20));
```

For sub-tables use dynamic table creation as shown below.

```sql
insert into memory_vm130_memory_buffered_collectd using memory tags('vm130', 'memory', ' buffer', 'collectd') values(1632979445, 3.0656);
```

The final system will have about 340 sub-tables and three super-tables. Note that if the use of concatenated tagged values causes the sub-table names to exceed the system limit (191 bytes), then some encoding (e.g., MD5) needs to be used to convert them to an acceptable length.

- **Multi-value model**

Suppose you want to take advantage of TDengine's multi-value modeling capabilities. In that case, you need first to meet the requirements that different collection quantities have the same collection frequency and can reach the **data write side simultaneously via a message queue**, thus ensuring writing multiple metrics at once using SQL statements. The metric's name is used as the name of the super table to create a multi-column model of data that has the same collection frequency and can arrive simultaneously. The names of the sub-tables are named using a fixed rule. Each of the above metrics contains only one measurement value, so converting it into a multi-value model is impossible.

## Data triage and application adaptation

Subscribe data from the message queue and start the adapted writer to write the data.

After writing the data starts for a while, you can use SQL statements to check whether the amount of data written meets the expected writing requirements. Use the following SQL statement to count the amount of data.

```sql
select count(*) from memory
```

After completing the query, if the data written does not differ from what is expected and there are no abnormal error messages from the writing program itself, you can confirm that the written data is complete and valid.

TDengine does not support querying, or data fetching using the OpenTSDB query syntax but does provide a counterpart for each of the OpenTSDB queries. The corresponding query processing can be adapted and applied in a manner obtained by examining Appendix 1. To fully understand the types of queries supported by TDengine, refer to the TDengine user manual.

TDengine supports the standard JDBC 3.0 interface for manipulating databases, but you can also use other types of high-level language connectors for querying and reading data to suit your application. Please read the user manual for specific operations and usage.

## Historical Data Migration

### 1. Use the tool to migrate data automatically

To facilitate historical data migration, we provide a plug-in for the data synchronization tool DataX, which can automatically write data into TDengine.The automatic data migration of DataX can only support the data migration process of a single value model.

For the specific usage of DataX and how to use DataX to write data to TDengine, please refer to [DataX-based TDengine Data Migration Tool](https://www.taosdata.com/blog/2021/10/26/3156.html).

After migrating via DataX, we found that we can significantly improve the efficiency of migrating historical data by starting multiple processes and migrating numerous metrics simultaneously. The following are some records of the migration process. I wish to use these for application migration as a reference.

| Number of datax instances (number of concurrent processes) | Migration record speed (pieces/second) |
| ----------------------------- | ------------------- -- |
| 1 | About 139,000 |
| 2 | About 218,000 |
| 3 | About 249,000 |
| 5 | About 295,000 |
| 10 | About 330,000 |

<br/> (Note: The test data comes from a single-node Intel(R) Core(TM) i7-10700 CPU@2.90GHz 16-core 64G hardware device, the channel and batchSize are 8 and 1000 respectively, and each record contains 10 tags)

### 2. Manual data migration

Suppose you need to use the multi-value model for data writing. In that case, you need to develop a tool to export data from OpenTSDB, confirm which timelines can be merged and imported into the same timeline, and then pass the time to import simultaneously through the SQL statement—written to the database.

Manual migration of data requires attention to the following two issues:

1) When storing the exported data on the disk, the disk needs to have enough storage space to accommodate the exported data files fully. Adopting the partial import mode to avoid the shortage of disk file storage after the total amount of data is exported. Preferentially export the timelines belonging to the same super table. Then the exported data files are imported into the TDengine system.

2) Under the full load of the system, if there are enough remaining computing and IO resources, establish a multi-threaded importing to maximize the efficiency of data migration. Considering the vast load that data parsing brings to the CPU, it is necessary to control the maximum number of parallel tasks to avoid the overall overload of the system triggered by importing historical data.

Due to the ease of operation of TDengine itself, there is no need to perform index maintenance and data format change processing in the entire process. The whole process only needs to be executed sequentially.

When wholly importing the historical data into TDengine, the two systems run simultaneously and then switch the query request to TDengine to achieve seamless application switching.

## Appendix 1: OpenTSDB query function correspondence table

### Avg

Equivalent function: avg

Example:

```sql
SELECT avg(val) FROM (SELECT first(val) FROM super_table WHERE ts >= startTime and ts <= endTime INTERVAL(20s) Fill(linear)) INTERVAL(20s)
```

Remark:

1. The value in Interval needs to be the same as the interval value in the outer query.
2. The interpolation processing in TDengine needs to use subqueries to assist in the completion. As shown above, it is enough to specify the interpolation type in the inner query. Since the interpolation of the values ​​in OpenTSDB uses linear interpolation, use fill( in the interpolation clause. linear) to declare the interpolation type. The following functions with the exact interpolation calculation requirements are processed by this method.
3. The parameter 20s in Interval indicates that the inner query will generate results according to a time window of 20 seconds. In an actual query, it needs to adjust to the time interval between different records. It ensures that producing interpolation results equivalent to the original data.
4. Due to the particular interpolation strategy and mechanism of OpenTSDB, the method of the first interpolation and then calculation in the aggregate query (Aggregate) makes the calculation results impossible to be utterly consistent with TDengine. But in the case of downsampling (Downsample), TDengine and OpenTSDB can obtain consistent results (since OpenTSDB performs aggregation and downsampling queries).

### Count

Equivalent function: count

Example:

```sql
select count(\*) from super_table_name;
```

### Dev

Equivalent function: stddev

Example:

```sql
Select stddev(val) from table_name
```

### Estimated percentiles

Equivalent function: apercentile

Example:

```sql
Select apercentile(col1, 50, “t-digest”) from table_name
```

Remark:

1. During the approximate query processing, OpenTSDB uses the t-digest algorithm by default, so in order to obtain the same calculation result, the algorithm used needs to be specified in the `apercentile()` function. TDengine can support two different approximation processing algorithms, declared by "default" and "t-digest" respectively.

### First

Equivalent function: first

Example:

```sql
Select first(col1) from table_name
```

### Last

Equivalent function: last

Example:

```sql
Select last(col1) from table_name
```

### Max

Equivalent function: max

Example:

```sql
Select max(value) from (select first(val) value from table_name interval(10s) fill(linear)) interval(10s)
```

Note: The Max function requires interpolation for the reasons described above.

### Min

Equivalent function: min

Example:

```sql
Select min(value) from (select first(val) value from table_name interval(10s) fill(linear)) interval(10s);
```

### MinMax

Equivalent function: max

```sql
Select max(val) from table_name
```

Note: This function has no interpolation requirements, so it can be directly calculated.

### MimMin

Equivalent function: min

```sql
Select min(val) from table_name
```

Note: This function has no interpolation requirements, so it can be directly calculated.

### Percentile

Equivalent function: percentile

Remark:

### Sum

Equivalent function: sum

```sql
Select max(value) from (select first(val) value from table_name interval(10s) fill(linear)) interval(10s)
```

Note: This function has no interpolation requirements, so it can be directly calculated.

### Zimsum

Equivalent function: sum

```sql
Select sum(val) from table_name
```

Note: This function has no interpolation requirements, so it can be directly calculated.

Complete example:

````json
// OpenTSDB query JSON
query = {
"start": 1510560000,
"end": 1515000009,
"queries": [{
"aggregator": "count",
"metric": "cpu.usage_user",
}]
}

// Equivalent query SQL:
SELECT count(*)
FROM `cpu.usage_user`
WHERE ts>=1510560000 AND ts<=1515000009
````

## Appendix 2: Resource Estimation Methodology

### Data generation environment

We still use the hypothetical environment from Chapter 4. There are three measurements. Respectively: the data writing rate of temperature and humidity is one record every 5 seconds, and the timeline is 100,000. The writing rate of air pollution is one record every 10 seconds, the timeline is 10,000, and the query request frequency is 500 QPS.

### Storage resource estimation

Assuming that the number of sensor devices that generate data and need to be stored is `n`, the frequency of data generation is `t` per second, and the length of each record is `L` bytes, the scale of data generated per day is `n * t * L` bytes. Assuming the compression ratio is `C`, the daily data size is `(n * t * L)/C` bytes. The storage resources are estimated to accommodate the data scale for 1.5 years. In the production environment, the compression ratio C of TDengine is generally between 5 and 7.
With additional 20% ​​redundancy, you can calculate the required storage resources:

```matlab
(n * t * L) * (365 * 1.5) * (1+20%)/C
````

Combined with the above calculation formula, bring the parameters into the formula, and the raw data scale generated every year is 11.8TB without considering the label information. Note that since tag information is associated with each timeline in TDengine, not every record. The scale of the amount of data to be recorded is somewhat reduced relative to the generated data, and this part of label data can be ignored as a whole. Assuming a compression ratio of 5, the size of the retained data ends up being 2.56 TB.

### Storage Device Selection Considerations

The hard disk should be capable of better random read performance. Considering using an SSD as much as possible is a better choice. A disk with better random read performance is a great help to improve the system's query performance and improve the query response performance as a whole system. To obtain better query performance, the performance index of the single-threaded random read IOPS of the hard disk device should not be lower than 1000, and it is better to reach 5000 IOPS or more. Recommend to use `fio` utility software to evaluate the running performance (please refer to Appendix 1 for specific usage) for the random IO read of the current device to confirm whether it can meet the requirements of random read of large files.

Hard disk writing performance has little effect on TDengine. The TDengine writing process adopts the append write mode, so as long as it has good sequential write performance, both SAS hard disks and SSDs in the general sense can well meet TDengine's requirements for disk write performance.

### Computational resource estimates

Due to the particularity of IoT data, after the frequency of data generation is consistent, the writing process of TDengine maintains a relatively fixed amount of resource consumption (computing and storage). According to the [TDengine Operation and Maintenance Guide](/operation/) description, the system consumes less than 1 CPU core at 22,000 writes per second.

In estimating the CPU resources consumed by the query, assuming that the application requires the database to provide 10,000 QPS, the CPU time consumed by each query is about 1 ms. The query provided by each core per second is 1,000 QPS, which satisfies 10,000 QPS. The query request requires at least 10 cores. For the system as a whole system to have less than 50% CPU load, the entire cluster needs twice as many as 10 cores or 20 cores.

### Memory resource estimation

The database allocates 16MB\*3 buffer memory for each Vnode by default. If the cluster system includes 22 CPU cores, TDengine will create 22 Vnodes (virtual nodes) by default. Each Vnode contains 1000 tables, which can accommodate all the tables. Then it takes about 1.5 hours to write a block, which triggers the drop, and no adjustment is required. A total of 22 Vnodes require about 1GB of memory cache. Considering the memory needed for the query, assuming that the memory overhead of each query is about 50MB, the memory required for 500 queries concurrently is about 25GB.

In summary, using a single 16-core 32GB machine or a cluster of 2 8-core 16GB machines is enough.

## Appendix 3: Cluster Deployment and Startup

TDengine provides a wealth of help documents to explain many aspects of cluster installation and deployment. Here is the list of corresponding document for your reference.

### Cluster Deployment

The first is TDengine installation. Download the latest stable version of TDengine from the official website, and install it. Please refer to the blog ["Installation and Uninstallation of Various Installation Packages of TDengine"](https://www.taosdata.com/blog/2019/08/09/566.html) for the various installation package formats.

Note that once the installation is complete, do not immediately start the `taosd` service, but start it after correctly configuring the parameters.

### Set running parameters and start the service

To ensure that the system can obtain the necessary information for regular operation. Please set the following vital parameters correctly on the server:

FQDN, firstEp, secondEP, dataDir, logDir, tmpDir, serverPort. For the specific meaning and setting requirements of each parameter, please refer to the document "[TDengine Cluster Installation and Management](/cluster/)"

Follow the same steps to set parameters on the nodes that need running, start the taosd service, and then add Dnodes to the cluster.

Finally, start `taos` and execute the `show dnodes` command. If you can see all the nodes that have joined the cluster, the cluster building process was successfully completed. For specific operation procedures and precautions, please refer to the document "[TDengine Cluster Installation and Management](/cluster/)".

## Appendix 4: Super Table Names

Since OpenTSDB's metric name has a dot (".") in it, for example, a metric with a name like "cpu.usage_user", the dot has a special meaning in TDengine and is a separator used to separate database and table names. TDengine also provides "escape" characters to allow users to use keywords or special separators (e.g., dots) in (super)table names. To use special characters, enclose the table name in escape characters, e.g.: `cpu.usage_user`. It is a valid (super) table name.

## Appendix 5: Reference Articles

1. [Using TDengine + collectd/StatsD + Grafana to quickly build an IT operation and maintenance monitoring system](/application/collectd/)
2. [Write collected data directly to TDengine through collectd](/third-party/collectd/)
