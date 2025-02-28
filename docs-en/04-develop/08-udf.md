---
sidebar_label: UDF
title: User Defined Functions
description: "Scalar functions and aggregate functions developed by users can be utilized by the query framework to expand the query capability"
---

In some use cases, the query capability required by application programs can't be achieved directly by builtin functions. With UDF, the functions developed by users can be utilized by query framework to meet some special requirements. UDF normally takes one column of data as input, but can also support the result of sub query as input.

From version 2.2.0.0, UDF programmed in C/C++ language can be supported by TDengine.

Two kinds of functions can be implemented by UDF: scalar function and aggregate function.

## Define UDF

### Scalar Function

Below function template can be used to define your own scalar function.

`void udfNormalFunc(char* data, short itype, short ibytes, int numOfRows, long long* ts, char* dataOutput, char* interBuf, char* tsOutput, int* numOfOutput, short otype, short obytes, SUdfInit* buf)`

`udfNormalFunc` is the place holder of function name, a function implemented based on the above template can be used to perform scalar computation on data rows. The parameters are fixed to control the data exchange between UDF and TDengine.

- Defintions of the parameters:

  - data：input data
  - itype：the type of input data, for details please refer to [type definition in column_meta](/reference/rest-api/), for example 4 represents INT
  - iBytes：the number of bytes consumed by each value in the input data
  - oType：the type of output data, similar to iType
  - oBytes：the number of bytes consumed by each value in the output data
  - numOfRows：the number of rows in the input data
  - ts: the column of timestamp corresponding to the input data
  - dataOutput：the buffer for output data, total size is `oBytes * numberOfRows`
  - interBuf：the buffer for intermediate result, its size is specified by `BUFSIZE` parameter when creating a UDF. It's normally used when the intermediate result is not same as the final result, it's allocated and freed by TDengine.
  - tsOutput：the column of timestamps corresponding to the output data; it can be used to output timestamp together with the output data if it's not NULL
  - numOfOutput：the number of rows in output data
  - buf：for the state exchange between UDF and TDengine

  [add_one.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/add_one.c) is one example of the simplest UDF implementations, i.e. one instance of the above `udfNormalFunc` template. It adds one to each value of a column passed in which can be filtered using `where` clause and outputs the result.

### Aggregate Function

Below function template can be used to define your own aggregate function.

`void abs_max_merge(char* data, int32_t numOfRows, char* dataOutput, int32_t* numOfOutput, SUdfInit* buf)`

`udfMergeFunc` is the place holder of function name, the function implemented with the above template is used to aggregate the intermediate result, only can be used in the aggregate query for STable.

Definitions of the parameters:

- data：array of output data, if interBuf is used it's an array of interBuf
- numOfRows：number of rows in `data`
- dataOutput：the buffer for output data, the size is same as that of the final result; If the result is not final, it can be put in the interBuf, i.e. `data`.
- numOfOutput：number of rows in the output data
- buf：for the state exchange between UDF and TDengine

[abs_max.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/abs_max.c) is an user defined aggregate function to get the maximum from the absolute value of a column.

The internal processing is that the data affected by the select statement will be divided into multiple row blocks and `udfNormalFunc`, i.e. `abs_max` in this case, is performed on each row block to generate the intermediate of each sub table, then `udfMergeFunc`, i.e. `abs_max_merge` in this case, is performed on the intermediate result of sub tables to aggregate to generate the final or intermediate result of STable. The intermediate result of STable is finally processed by `udfFinalizeFunc` to generate the final result, which contain either 0 or 1 row.

Other typical scenarios, like covariance, can also be achieved by aggregate UDF.

### Finalize

Below function template can be used to finalize the result of your own UDF, normally used when interBuf is used.

`void abs_max_finalize(char* dataOutput, char* interBuf, int* numOfOutput, SUdfInit* buf)`

`udfFinalizeFunc` is the place holder of function name, definitions of the parameter are as below:

- dataOutput：buffer for output data
- interBuf：buffer for intermediate result, can be used as input for next processing step
- numOfOutput：number of output data, can only be 0 or 1 for aggregate function
- buf：for state exchange between UDF and TDengine

## UDF Conventions

The naming of 3 kinds of UDF, i.e. udfNormalFunc, udfMergeFunc, and udfFinalizeFunc is required to have same prefix, i.e. the actual name of udfNormalFunc, which means udfNormalFunc doesn't need a suffix following the function name. While udfMergeFunc should be udfNormalFunc followed by `_merge`, udfFinalizeFunc should be udfNormalFunc followed by `_finalize`. The naming convention is part of UDF framework, TDengine follows this convention to invoke corresponding actual functions.\

According to the kind of UDF to implement, the functions that need to be implemented are different.

- Scalar function：udfNormalFunc is required
- Aggregate function：udfNormalFunc, udfMergeFunc (if query on STable) and udfFinalizeFunc are required

To be more accurate, assuming we want to implement a UDF named "foo". If the function is a scalar function, what we really need to implement is `foo`; if the function is aggregate function, we need to implement `foo`, `foo_merge`, and `foo_finalize`. For aggregate UDF, even though one of the three functions is not necessary, there must be an empty implementation.

## Compile UDF

The source code of UDF in C can't be utilized by TDengine directly. UDF can only be loaded into TDengine after compiling to dynamically linked library.

For example, the example UDF `add_one.c` mentioned in previous sections need to be compiled into DLL using below command on Linux Shell.

```bash
gcc -g -O0 -fPIC -shared add_one.c -o add_one.so
```

The generated DLL file `dd_one.so` can be used later when creating UDF. It's recommended to use GCC not older than 7.5.

## Create and Use UDF

### Create UDF

SQL command can be executed on the same hos where the generated UDF DLL resides to load the UDF DLL into TDengine, this operation can't be done through REST interface or web console. Once created, all the clients of the current TDengine can use these UDF functions in their SQL commands. UDF are stored in the management node of TDengine. The UDFs loaded in TDengine would be still available after TDengine is restarted.

When creating UDF, it needs to be clarified as either scalar function or aggregate function. If the specified type is wrong, the SQL statements using the function would fail with error. Besides, the input type and output type don't need to be same in UDF, but the input data type and output data type need to be consistent with the UDF definition.

- Create Scalar Function

```sql
CREATE FUNCTION ids(X) AS ids(Y) OUTPUTTYPE typename(Z) [ BUFSIZE B ];
```

- ids(X)：the function name to be sued in SQL statement, must be consistent with the function name defined by `udfNormalFunc`
- ids(Y)：the absolute path of the DLL file including the implementation of the UDF, the path needs to be quoted by single or double quotes
- typename(Z)：the output data type, the value is the literal string of the type
- B：the size of intermediate buffer, in bytes; it's an optional parameter and the range is [0,512]

For example, below SQL statement can be used to create a UDF from `add_one.so`.

```sql
CREATE FUNCTION add_one AS "/home/taos/udf_example/add_one.so" OUTPUTTYPE INT;
```

- Create Aggregate Function

```sql
CREATE AGGREGATE FUNCTION ids(X) AS ids(Y) OUTPUTTYPE typename(Z) [ BUFSIZE B ];
```

- ids(X)：the function name to be sued in SQL statement, must be consistent with the function name defined by `udfNormalFunc`
- ids(Y)：the absolute path of the DLL file including the implementation of the UDF, the path needs to be quoted by single or double quotes
- typename(Z)：the output data type, the value is the literal string of the type 此
- B：the size of intermediate buffer, in bytes; it's an optional parameter and the range is [0,512]

For details about how to use intermediate result, please refer to example program [demo.c](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/demo.c).

For example, below SQL statement can be used to create a UDF rom `demo.so`.

```sql
CREATE AGGREGATE FUNCTION demo AS "/home/taos/udf_example/demo.so" OUTPUTTYPE DOUBLE bufsize 14;
```

### Manage UDF

- Delete UDF

```
DROP FUNCTION ids(X);
```

- ids(X)：same as that in `CREATE FUNCTION` statement

```sql
DROP FUNCTION add_one;
```

- Show Available UDF

```sql
SHOW FUNCTIONS;
```

### Use UDF

The function name specified when creating UDF can be used directly in SQL statements, just like builtin functions.

```sql
SELECT X(c) FROM table/STable;
```

The above SQL statement invokes function X for column c.

## Restrictions for UDF

In current version there are some restrictions for UDF

1. Only Linux is supported when creating and invoking UDF for both client side and server side
2. UDF can't be mixed with builtin functions
3. Only one UDF can be used in a SQL statement
4. Single column is supported as input for UDF
5. Once created successfully, UDF is persisted in MNode of TDengineUDF
6. UDF can't be created through REST interface
7. The function name used when creating UDF in SQL must be consistent with the function name defined in the DLL, i.e. the name defined by `udfNormalFunc`
8. The name name of UDF name should not conflict with any of builtin functions

## Examples

### Scalar function example [add_one](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/add_one.c)

<details>
<summary>add_one.c</summary>

```c
{{#include tests/script/sh/add_one.c}}
```

</details>

### Aggregate function example [abs_max](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/abs_max.c)

<details>
<summary>abs_max.c</summary>

```c
{{#include tests/script/sh/abs_max.c}}
```

</details>

### Example for using intermediate result [demo](https://github.com/taosdata/TDengine/blob/develop/tests/script/sh/demo.c)

<details>
<summary>demo.c</summary>

```c
{{#include tests/script/sh/demo.c}}
```

</details>
