CPA Example
================================
[![Build Status](https://drone.io/github.com/TU-Berlin/cpa-demo/status.png)](https://drone.io/github.com/TU-Berlin/cpa-demo/latest)
# Run
* compile the maven project
* upload the Jar file to the stratosphere web interface

# Usage
* WikiSim: [DATASET] [OUTPUT] [ALPHA1, ALPHA2, ...] [REDUCER-THRESHOLD] [COMBINER-THRESHOLD]
** e.g.: hdfs://cluster/wikidump.xml hdfs://cluster/results.out 0.5,1.0,1.5,2.0 10 5

* Evaluation: [OUTPUT] [SEEALSO-DATASET] [WIKISIM-DATASET] [MLT-DATASET] [LINKS/NOFILTER] [TOP-K1, TOP-K2, ...] [CPA-FIELD]
** e.g.: hdfs://cluster/eval.out hdfs://cluster/seealso.csv hdfs://cluster/results.out hdfs://cluster/mlt.csv nofilter 10,5,1 8

# TODO
document program execution
first successfull local run (dopa-1):

```
dopa-user@dopa-1:~/ms/cpa-demo$ time ../vicotest/s/bin/stratosphere run -w -a file:///home/dopa-user/ms/wikienmath.xml file:///home/dopa-user/ms/cpa.csv -j target/cpa-0.0.1.jar -c de.tuberlin.dima.schubotz.cpa.RelationFinder
04/27/2014 13:41:00:    Job execution switched to status SCHEDULED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (1/8) switched to SCHEDULED
04/27/2014 13:41:00:    Reduce(Filter) (1/8) switched to SCHEDULED
04/27/2014 13:41:00:    DataSink(Output) (1/8) switched to SCHEDULED
04/27/2014 13:41:00:    Reduce(Filter) (2/8) switched to SCHEDULED
04/27/2014 13:41:00:    DataSink(Output) (2/8) switched to SCHEDULED
04/27/2014 13:41:00:    Reduce(Filter) (3/8) switched to SCHEDULED
04/27/2014 13:41:00:    DataSink(Output) (3/8) switched to SCHEDULED
04/27/2014 13:41:00:    Reduce(Filter) (4/8) switched to SCHEDULED
04/27/2014 13:41:00:    DataSink(Output) (4/8) switched to SCHEDULED
04/27/2014 13:41:00:    Reduce(Filter) (5/8) switched to SCHEDULED
04/27/2014 13:41:00:    DataSink(Output) (5/8) switched to SCHEDULED
04/27/2014 13:41:00:    Reduce(Filter) (6/8) switched to SCHEDULED
04/27/2014 13:41:00:    DataSink(Output) (6/8) switched to SCHEDULED
04/27/2014 13:41:00:    Reduce(Filter) (7/8) switched to SCHEDULED
04/27/2014 13:41:00:    DataSink(Output) (7/8) switched to SCHEDULED
04/27/2014 13:41:00:    Reduce(Filter) (8/8) switched to SCHEDULED
04/27/2014 13:41:00:    DataSink(Output) (8/8) switched to SCHEDULED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (2/8) switched to SCHEDULED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (3/8) switched to SCHEDULED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (4/8) switched to SCHEDULED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (5/8) switched to SCHEDULED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (6/8) switched to SCHEDULED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (7/8) switched to SCHEDULED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (8/8) switched to SCHEDULED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (6/8) switched to ASSIGNED
04/27/2014 13:41:00:    Reduce(Filter) (5/8) switched to ASSIGNED
04/27/2014 13:41:00:    DataSink(Output) (8/8) switched to ASSIGNED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (8/8) switched to ASSIGNED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (2/8) switched to ASSIGNED
04/27/2014 13:41:00:    DataSink(Output) (7/8) switched to ASSIGNED
04/27/2014 13:41:00:    Reduce(Filter) (7/8) switched to ASSIGNED
04/27/2014 13:41:00:    DataSink(Output) (4/8) switched to ASSIGNED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (7/8) switched to ASSIGNED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (4/8) switched to ASSIGNED
04/27/2014 13:41:00:    DataSink(Output) (1/8) switched to ASSIGNED
04/27/2014 13:41:00:    DataSink(Output) (2/8) switched to ASSIGNED
04/27/2014 13:41:00:    Reduce(Filter) (4/8) switched to ASSIGNED
04/27/2014 13:41:00:    DataSink(Output) (5/8) switched to ASSIGNED
04/27/2014 13:41:00:    DataSink(Output) (3/8) switched to ASSIGNED
04/27/2014 13:41:00:    Reduce(Filter) (2/8) switched to ASSIGNED
04/27/2014 13:41:00:    Reduce(Filter) (1/8) switched to ASSIGNED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (1/8) switched to ASSIGNED
04/27/2014 13:41:00:    DataSink(Output) (6/8) switched to ASSIGNED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (5/8) switched to ASSIGNED
04/27/2014 13:41:00:    Reduce(Filter) (6/8) switched to ASSIGNED
04/27/2014 13:41:00:    Reduce(Filter) (3/8) switched to ASSIGNED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (3/8) switched to ASSIGNED
04/27/2014 13:41:00:    Reduce(Filter) (8/8) switched to ASSIGNED
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (1/8) switched to READY
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (2/8) switched to READY
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (3/8) switched to READY
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (4/8) switched to READY
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (5/8) switched to READY
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (6/8) switched to READY
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (7/8) switched to READY
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (8/8) switched to READY
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (1/8) switched to STARTING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (2/8) switched to STARTING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (3/8) switched to STARTING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (4/8) switched to STARTING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (5/8) switched to STARTING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (6/8) switched to STARTING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (7/8) switched to STARTING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (8/8) switched to STARTING
04/27/2014 13:41:00:    Job execution switched to status RUNNING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (1/8) switched to RUNNING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (8/8) switched to RUNNING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (7/8) switched to RUNNING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (6/8) switched to RUNNING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (5/8) switched to RUNNING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (4/8) switched to RUNNING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (3/8) switched to RUNNING
04/27/2014 13:41:00:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (2/8) switched to RUNNING
04/27/2014 13:42:27:    Reduce(Filter) (4/8) switched to READY
04/27/2014 13:42:27:    Reduce(Filter) (4/8) switched to STARTING
04/27/2014 13:42:27:    Reduce(Filter) (4/8) switched to RUNNING
04/27/2014 13:42:27:    Reduce(Filter) (1/8) switched to READY
04/27/2014 13:42:27:    Reduce(Filter) (1/8) switched to STARTING
04/27/2014 13:42:27:    Reduce(Filter) (1/8) switched to RUNNING
04/27/2014 13:42:28:    Reduce(Filter) (3/8) switched to READY
04/27/2014 13:42:28:    Reduce(Filter) (3/8) switched to STARTING
04/27/2014 13:42:28:    Reduce(Filter) (3/8) switched to RUNNING
04/27/2014 13:42:28:    Reduce(Filter) (2/8) switched to READY
04/27/2014 13:42:28:    Reduce(Filter) (2/8) switched to STARTING
04/27/2014 13:42:28:    Reduce(Filter) (2/8) switched to RUNNING
04/27/2014 13:42:29:    Reduce(Filter) (6/8) switched to READY
04/27/2014 13:42:29:    Reduce(Filter) (6/8) switched to STARTING
04/27/2014 13:42:29:    Reduce(Filter) (6/8) switched to RUNNING
04/27/2014 13:42:29:    Reduce(Filter) (8/8) switched to READY
04/27/2014 13:42:29:    Reduce(Filter) (8/8) switched to STARTING
04/27/2014 13:42:29:    Reduce(Filter) (8/8) switched to RUNNING
04/27/2014 13:42:29:    Reduce(Filter) (7/8) switched to READY
04/27/2014 13:42:29:    Reduce(Filter) (7/8) switched to STARTING
04/27/2014 13:42:29:    Reduce(Filter) (7/8) switched to RUNNING
04/27/2014 13:42:30:    Reduce(Filter) (5/8) switched to READY
04/27/2014 13:42:30:    Reduce(Filter) (5/8) switched to STARTING
04/27/2014 13:42:30:    Reduce(Filter) (5/8) switched to RUNNING
04/27/2014 13:44:51:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (5/8) switched to FINISHING
04/27/2014 13:47:16:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (4/8) switched to FINISHING
04/27/2014 13:48:19:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (3/8) switched to FINISHING
04/27/2014 13:49:37:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (7/8) switched to FINISHING
04/27/2014 13:49:41:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (2/8) switched to FINISHING
04/27/2014 13:50:14:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (1/8) switched to FINISHING
04/27/2014 13:57:45:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (6/8) switched to FINISHING
04/27/2014 14:06:14:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (8/8) switched to FINISHING
04/27/2014 14:06:41:    DataSink(Output) (6/8) switched to READY
04/27/2014 14:06:41:    DataSink(Output) (6/8) switched to STARTING
04/27/2014 14:06:41:    DataSink(Output) (6/8) switched to RUNNING
04/27/2014 14:06:42:    DataSink(Output) (5/8) switched to READY
04/27/2014 14:06:42:    DataSink(Output) (5/8) switched to STARTING
04/27/2014 14:06:42:    DataSink(Output) (5/8) switched to RUNNING
04/27/2014 14:06:44:    DataSink(Output) (1/8) switched to READY
04/27/2014 14:06:44:    DataSink(Output) (1/8) switched to STARTING
04/27/2014 14:06:44:    DataSink(Output) (1/8) switched to RUNNING
04/27/2014 14:06:47:    DataSink(Output) (7/8) switched to READY
04/27/2014 14:06:47:    DataSink(Output) (7/8) switched to STARTING
04/27/2014 14:06:47:    DataSink(Output) (7/8) switched to RUNNING
04/27/2014 14:06:48:    DataSink(Output) (4/8) switched to READY
04/27/2014 14:06:48:    DataSink(Output) (4/8) switched to STARTING
04/27/2014 14:06:48:    DataSink(Output) (4/8) switched to RUNNING
04/27/2014 14:06:51:    DataSink(Output) (3/8) switched to READY
04/27/2014 14:06:51:    DataSink(Output) (3/8) switched to STARTING
04/27/2014 14:06:51:    DataSink(Output) (3/8) switched to RUNNING
04/27/2014 14:06:53:    DataSink(Output) (8/8) switched to READY
04/27/2014 14:06:53:    DataSink(Output) (8/8) switched to STARTING
04/27/2014 14:06:53:    DataSink(Output) (8/8) switched to RUNNING
04/27/2014 14:06:56:    DataSink(Output) (2/8) switched to READY
04/27/2014 14:06:56:    DataSink(Output) (2/8) switched to STARTING
04/27/2014 14:06:56:    DataSink(Output) (2/8) switched to RUNNING
04/27/2014 14:10:13:    Reduce(Filter) (6/8) switched to FINISHING
04/27/2014 14:10:13:    DataSink(Output) (6/8) switched to FINISHING
04/27/2014 14:10:13:    DataSink(Output) (6/8) switched to FINISHED
04/27/2014 14:10:13:    Reduce(Filter) (6/8) switched to FINISHED
04/27/2014 14:10:19:    Reduce(Filter) (5/8) switched to FINISHING
04/27/2014 14:10:19:    DataSink(Output) (5/8) switched to FINISHING
04/27/2014 14:10:19:    DataSink(Output) (5/8) switched to FINISHED
04/27/2014 14:10:19:    Reduce(Filter) (5/8) switched to FINISHED
04/27/2014 14:10:19:    Reduce(Filter) (1/8) switched to FINISHING
04/27/2014 14:10:19:    DataSink(Output) (1/8) switched to FINISHING
04/27/2014 14:10:19:    DataSink(Output) (1/8) switched to FINISHED
04/27/2014 14:10:19:    Reduce(Filter) (1/8) switched to FINISHED
04/27/2014 14:10:19:    Reduce(Filter) (7/8) switched to FINISHING
04/27/2014 14:10:19:    DataSink(Output) (7/8) switched to FINISHING
04/27/2014 14:10:19:    DataSink(Output) (7/8) switched to FINISHED
04/27/2014 14:10:20:    Reduce(Filter) (7/8) switched to FINISHED
04/27/2014 14:10:21:    Reduce(Filter) (4/8) switched to FINISHING
04/27/2014 14:10:21:    DataSink(Output) (4/8) switched to FINISHING
04/27/2014 14:10:21:    DataSink(Output) (4/8) switched to FINISHED
04/27/2014 14:10:22:    Reduce(Filter) (4/8) switched to FINISHED
04/27/2014 14:10:24:    Reduce(Filter) (3/8) switched to FINISHING
04/27/2014 14:10:24:    DataSink(Output) (3/8) switched to FINISHING
04/27/2014 14:10:24:    DataSink(Output) (3/8) switched to FINISHED
04/27/2014 14:10:24:    Reduce(Filter) (3/8) switched to FINISHED
04/27/2014 14:10:25:    Reduce(Filter) (8/8) switched to FINISHING
04/27/2014 14:10:25:    DataSink(Output) (8/8) switched to FINISHING
04/27/2014 14:10:25:    DataSink(Output) (8/8) switched to FINISHED
04/27/2014 14:10:25:    Reduce(Filter) (8/8) switched to FINISHED
04/27/2014 14:10:26:    Reduce(Filter) (2/8) switched to FINISHING
04/27/2014 14:10:26:    DataSink(Output) (2/8) switched to FINISHING
04/27/2014 14:10:26:    DataSink(Output) (2/8) switched to FINISHED
04/27/2014 14:10:26:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (3/8) switched to FINISHED
04/27/2014 14:10:26:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (6/8) switched to FINISHED
04/27/2014 14:10:26:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (1/8) switched to FINISHED
04/27/2014 14:10:26:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (4/8) switched to FINISHED
04/27/2014 14:10:26:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (7/8) switched to FINISHED
04/27/2014 14:10:26:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (2/8) switched to FINISHED
04/27/2014 14:10:26:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (8/8) switched to FINISHED
04/27/2014 14:10:26:    CHAIN DataSource(Dumps) -> Map(Processing Documents) -> Combine(Filter) (5/8) switched to FINISHED
04/27/2014 14:10:26:    Job execution switched to status FINISHED
Job Runtime: 1766242

real    29m28.160s
user    0m1.752s
sys     0m0.508s
```
