# TODOs

- [ ] see if possible to get rid of the dependency on "parquet-avro"
- [ ] don't use 0L as default for stage start/end
- [x] for Sink, use an API consistent with FileWriter (write, flush, close)
- [ ] in sinks batching implementations, use different counter for different reports, and ideally use bytes instead of number of reports
- [ ] use tmp folder for parquet sink tests



- [x] remove filter completely
- [ ] batch and dump API (make it Seq)
- [ ] batch and dump API default implementation (parquet / json)
- [ ] retrieve id of Spark SQL query (as per execution plan () notation)
- [P] onApplicationEnd (for shutdown hook)
- [x] event stage end, update README
- [x] make sink a type

- [x] Add diagram to explain basic architecture
- [ ] Use it in PRD projects (json2star, snowflake-push, ...)
- [ ] Improve README to include comparison with other similar solution
- [x] Add test case with a join
- [x] This project must be dependency-free, so there is a fix to do on the logger library
- [ ] Review the CappedMap (JMH benchmark?)
- [ ] Add the missing link of SQL queries with children SQL queries
- [ ] Add the missing link SQL queries with stages
- [ ] Add use case catalog / examples
    - read amplification
    - hashbroadcastjoin is not being used
    - compare scan parquet, v1 v2
    - within a SQL query, which job took longer and why
    - provide the maximum SQL in terms of cost (find the big fish)
- [ ] Investigate if possible to persist SparkUI logs as a complementary approach with
    ```
    From https://docs.databricks.com/en/clusters/configure.html#cluster-log-delivery. 
    For example, if the log path is dbfs:/cluster-logs, the log files for a specific cluster will be 
    stored in dbfs:/cluster-logs/<cluster-name> and the individual event logs will be stored 
    in dbfs:/cluster-logs/<cluster-name>/eventlog/<cluster-name-cluster-ip>/<log-id>/.
    Settings:
    - spark.eventLog.enabled true
    - spark.eventLog.dir dbfs:/databricks/unravel/eventLogs/
    ```
