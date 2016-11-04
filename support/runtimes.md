# Average Runtime for Flink Jobs

## Setup

The experiment was performed on a cluster of 10 IBM Power 730 (8231-E2B) servers. Each machine had 2x3.7 GHz POWER7 processors with 6 cores (12 cores in total), 2 x 73.4 GB 15K RPM SAS SFF Disk Drive, 4 x 600 GB 10K RPM SAS SFF Disk Drive and 64 GB of RAM.
We used Apache Flink v0.8 and Hadoop v2.0. The text-based similarity measure was evaluated with Elasticsearch v1.4.2. All versions were the latest stable releases at the time of writing. We used the software’s default settings.

## Runtime

### JCDL Paper (Flink v0.8)

- MLT
    - Indexing (7h 30min)
    - Retreival
        - 1000 queries / min
        - 3,225,355 queries
        - 53.75 hrs
- CPA
    - WikiSim (Results only): 3h 10min
    - WikiSim with redirects: 7 hrs 45 mins
- Evaluation
    - See also extraction (1h 15min)
    - See also evaluation: 45 mins
    - ClickStream evaluation

### Mobile Recommendations (Flink 1.1.0)

Settings
```
taskmanager.memory.fraction: 0.6
taskmanager.numberOfTaskSlots: 64
parallelization.default: 300
```

- 1GB Dump Test
    - WikiSim GroupReduce   15m 46s
        - FlatMap out       736,077,120 / 45.0 GB

    - WikiSim Reduce        15m 38s
        - Hint HashCombiner
        - FlatMap out       730,576,729 / 39.2 GB

- Full Dump
    - taskmanager.memory.fraction: 0.85
    - parallelism 100
        - WikiSim Reduce        1h 25m
            - FlatMap out       11,216,905,047 / 638 GB

        - WikiSim GroupReduce   1h 46m
            - FlatMap out       11,423,198,553 / 734 GB



- CPA
    - WikiSim (result only): 1h 4m
        - GroupReduce
            - Records sent (before Reduce, parallelism 200): 3,025,106,250 (194 GB)
            - Records sent (before Reduce, parallelism 100): 3,024,990,488 (194 GB)

        - Reduce (no hint, p=100)
            - Records sent:

    - WikiSim with redirects:
        - Parallelism 250: 1h 6m
        - Parallelism 350: 1h 5m

