# Average Runtime for Flink Jobs

## Setup

The experiment was performed on a cluster of 10 IBM Power 730 (8231-E2B) servers. Each machine had 2x3.7 GHz POWER7 processors with 6 cores (12 cores in total), 2 x 73.4 GB 15K RPM SAS SFF Disk Drive, 4 x 600 GB 10K RPM SAS SFF Disk Drive and 64 GB of RAM.
We used Apache Flink v0.8 and Hadoop v2.0. The text-based similarity measure was evaluated with Elasticsearch v1.4.2. All versions were the latest stable releases at the time of writing. We used the software’s default settings.

## Runtime

### JCDL Paper

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

### Mobile Recommendations

- Flink 1.1.0

- CPA
    - WikiSim (result only):
        - Records sent (before Reduce):