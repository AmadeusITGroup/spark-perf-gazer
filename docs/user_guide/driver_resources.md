# Driver Resources

PerfGazer runs entirely on the Spark driver: reports are collected, buffered, and written there. In HDFS mode (a remote `destination` URI), the driver also stages files on its local disk and copies them to remote storage. Keep the following in mind so PerfGazer does not put undue pressure on the driver.

## Local staging disk (HDFS mode)

In HDFS mode, reports are first written to a local staging directory on the driver before being promoted to the remote destination. Size `spark.perfgazer.sink.json.fileSizeLimit` (or `JsonSink.Config(fileSizeLimit = ...)` in code) so staged files cannot fill the driver's local disk: at any moment the staging area holds roughly one file per enabled report type up to this limit, plus any files retained after a failed promotion.

## Storage locality (HDFS mode)

Promotion copies each completed file over the network to the remote destination. To keep these copies fast, pick a destination that is network-close to the driver — ideally in the same region or zone (for example, the same virtual network or resource group as the cluster). A distant destination increases copy time and can slow down the writer threads.

## Driver CPU

Reports are written asynchronously, with one dedicated writer thread per enabled report type (SQL, jobs, stages, tasks). Make sure the driver has enough CPU headroom so this background work does not compete with your application. This applies in both POSIX and HDFS mode; in HDFS mode the writer threads additionally perform the network copy, so the headroom matters more. Disabling report types you don't need (for example `spark.perfgazer.tasks.enabled=false`, or `PerfGazerConfig(tasksEnabled = false)` in code) reduces the number of writer threads.
