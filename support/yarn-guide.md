# WikiSim on YARN

This is a simple guide how to run the WikiSim Flink job on Apache Hadoop YARN.

**Requirements**
- at least Apache Hadoop 2.2
- HDFS (Hadoop Distributed File System) (or another distributed file system supported by Hadoop)


```
# Start YARN ( ResourceManager - http://localhost:8088/ )
sbin/start-yarn.sh

# Option 1)
# - Start Flink Yarn Session
flink/bin/yarn-session.sh -n 4 -jm 1024 -tm 4096
# - Run Flink Job
flink/bin/flink run ...

# Option 2)
# - Single Job Session
flink/bin/flink run


# Stop YARN
sbin/stop-yarn.sh
```

