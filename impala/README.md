使用impyla的thrift文件编译，删除ImpalaService.thrift里的SkewedInfo，原因go map的key不能为func/map/slice。

这个结构体是存储表的 偏移量数据 对于基础查询 无用，所以这里就删除了

```go
// this object holds all the information about skewed table
struct SkewedInfo {
    1: list<string> skewedColNames, // skewed column names
    2: list<list<string>> skewedColValues, //skewed values
    3: map<list<string>, string> skewedColValueLocationMaps, //skewed value to location mappings
}

// this object holds all the information about physical storage of the data belonging to a table
struct StorageDescriptor {
  1: list<FieldSchema> cols,  // required (refer to types defined above)
  2: string location,         // defaults to <warehouse loc>/<db loc>/tablename
  3: string inputFormat,      // SequenceFileInputFormat (binary) or TextInputFormat`  or custom format
  4: string outputFormat,     // SequenceFileOutputFormat (binary) or IgnoreKeyTextOutputFormat or custom format
  5: bool   compressed,       // compressed or not
  6: i32    numBuckets,       // this must be specified if there are any dimension columns
  7: SerDeInfo    serdeInfo,  // serialization and deserialization information
  8: list<string> bucketCols, // reducer grouping columns and clustering columns and bucketing columns`
  9: list<Order>  sortCols,   // sort order of the data in each bucket
  10: map<string, string> parameters, // any user supplied key value hash
  11: optional SkewedInfo skewedInfo, // skewed information
  12: optional bool   storedAsSubDirectories       // stored as subdirectories or not
}
```

* 每个查询都是 绑定在 beeswax.QueryHandle