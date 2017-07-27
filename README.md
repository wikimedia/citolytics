# Citolytics - Citation Analysis for Wikipedia with Apache Flink

This repository contains all resources used in the research paper [Evaluating Link-based Recommendations for Wikipedia](https://github.com/wikimedia/citolytics/releases/download/v0.0.2/paper.pdf) including the [Apache Flink](https://flink.apache.org/) based implementation of [Co-Citation](https://en.wikipedia.org/wiki/Co-citation) (CoCit) and [Co-Citation Proximity Analysis](https://en.wikipedia.org/wiki/Co-citation_Proximity_Analysis) (CPA) for Wikipedia, the evaluation application with several performance measures, the final and intermediate result data sets and additional tools for testing and debugging.

The evaluation was performed on the English Wikipedia XML dump from [September 2014](https://archive.org/details/wikimedia-mediatar). Resources regarding the [Apache Lucence MoreLikeThis](https://lucene.apache.org/) baseline can be found in a [separate repository](https://github.com/mschwarzer/Wikipedia2Lucene).



[![Build Status](https://travis-ci.org/wikimedia/citolytics.svg?branch=master)](https://travis-ci.org/wikimedia/citolytics) [![Coverage Status](https://coveralls.io/repos/github/wikimedia/citolytics/badge.svg?branch=master)](https://coveralls.io/github/wikimedia/citolytics?branch=master)

### Requirements

- Maven (v3.0+)
- Apache Flink (v1.3.1)

### Contents

- CPA and CoCit computation on Wikipedia text corpus
- Extraction of "See also" link as gold standard
- "See also" and click stream based evaluation
- Performance evaluation with several measures
    - Mean Average Precision (default)
    - Top-K (always included)
    - Mean Reciprocal Rank (set [ENABLE-MRR] parameter)
    - Click Through Rate (default for click streams)
- Link graph extraction for result pairs
- Wikipedia article statistics generator
- CirrusSearch output generator

### Evaluation

See Also Evaluation | Clickstream Evaluation
:-------------------------:|:-------------------------:
![MAP Evaluation](evaluation/figure5_map-overall_s.png) | ![CTR Evaluation](evaluation/figure6_ctr-overall_s.png)

We performed several evaluations on the recommendation quality of CPA, CoCit and MLT. Additional plots and raw and aggregated results can be found [here](evaluation).

### Build

```
mvn clean install -Dmaven.test.skip=true
```

### Run

Run Apache Flink jobs separately from jar-file (target/cpa-0.1.jar) and set -c parameter for each class.

#### Available Flink Jobs (Classes)

- **CPA/CoCit Computation**: org.wikipedia.citolytics.cpa.WikiSim
```
# Parameters
--input <local-or-hdfs-path>    Path to Wikipedia XML Dump (*)
--output <local-or-hdfs-path>   Write output to this path (*)
--alpha <double,double,...>     Alpha values for CPI computation (default: 1.5)
--format <year>                 Year of Wikipedia dump (2013 or 2016, default: 2013)
--redirects <local-or-hdfs-path>    Path redirects data set (default: none)
--reducer-threshold <int>       Set for cluster debugging (default: 1)
--combiner-threshold <int>      Set for cluster debugging (default: 1)

# Example
./bin/flink run -c org.wikipedia.citolytics.cpa.WikiSim cpa.jar --input hdfs://cluster/wikidump.xml \
    --output hdfs://cluster/results.out \
    --alpha 0.5,1.0,1.5,2.0
    --redirects hdfs://cluster/redirects.csv
```

- **SeeAlsoEvaluation**: org.wikipedia.citolytics.seealso.SeeAlsoEvaluation
```
# Parameters
--wikisim <local-or-hdfs-path>    Path to WikiSim output (*)
--output <local-or-hdfs-path>   Write output to this path (*)
--gold <local-or-hdfs-path>    Path "See also" data set (*)
--links <local-or-hdfs-path>    Path links data set (default: none)
--score <int>   Field id for CPA score (default: 5)
--page-a <int>  Field id for page A (default: 1)
--page-b <int>  Field id for page B (default: 2)
--enable-mrr    Set to enable mean-reciprocal-rank as performance measure
--topk <int>    Number of top-K results used for evaluation (default: 10)

# Example
./bin/flink run -c org.wikipedia.citolytics.seealso.SeeAlsoEvaluation cpa.jar --input hdfs://cluster/wikisim.out \
    --output hdfs://cluster/evaluation.out \
    --gold hdfs://cluster/see-also.csv \
    --topk 10
```

- **SeeAlsoExtractor**: org.wikipedia.citolytics.seealso.SeeAlsoExtractor
```
# Parameters
--input <local-or-hdfs-path>    Path to Wikipedia XML Dump (*)
--output <local-or-hdfs-path>   Write output to this path (*)
--redirects <local-or-hdfs-path>    Set to resolve redirects (default: disabled)

# Example
./bin/flink run -c org.wikipedia.citolytics.seealso.SeeAlsoExtractor --input hdfs://cluster/wikidump.xml \
    --output hdfs://cluster/seealso.out \
    --redirects hdfs://cluster/redirects.out
```

- **Extract Redirects**: org.wikipedia.citolytics.redirects.RedirectExtractor
```
# Parameters
--input <local-or-hdfs-path>    Path to Wikipedia XML Dump (*)
--output <local-or-hdfs-path>   Write output to this path (*)

# Example
./bin/flink run -c org.wikipedia.citolytics.redirects.RedirectExtractor --input hdfs://cluster/wikidump.xml \
    --output hdfs://cluster/redirects.out
```

- **ClickStreamEvaluation**: org.wikipedia.citolytics.clickstream.ClickStreamEvaluation
```
# Parameters
--wikisim <local-or-hdfs-path>    Path to WikiSim output (*)
--output <local-or-hdfs-path>   Write output to this path (*)
--gold <local-or-hdfs-path>    Path click stream data set (*)
--score <int>   Field id for CPA score (default: 5)
--page-a <int>  Field id for page A (default: 1)
--page-b <int>  Field id for page B (default: 2)
--topk <int>    Number of top-K results used for evaluation (default: 10)

# Example
./bin/flink run -c org.wikipedia.citolytics.clickstream.ClickStreamEvaluation cpa.jar --input hdfs://cluster/wikisim.out \
    --output hdfs://cluster/evaluation.out \
    --gold hdfs://cluster/wiki-clickstream.tsv \
    --topk 10
```

- Parameters with * are required.
- For more Flink jobs see [/support/flink-jobs/README.md](https://github.com/TU-Berlin/cpa-demo/blob/master/support/flink-jobs.md)
- For runtime information see [/support/runtimes.md](https://github.com/TU-Berlin/cpa-demo/blob/master/support/runtimes.md)

### Evaluation Example
To evaluate and generate the data used in our publication, you need to run the following Flink jobs in the order below. Paths to JAR and HDFS need to be adjusted depending on your setup.

#### 1. Extract Redirects from Wikipedia
```
./bin/flink run -c RedirectExtractor \
    /cpa.jar \
    hdfs:///datasets/enwiki-latest-pages-meta-current.xml \
    hdfs:///wikisim/intermediate/redirects \
```

#### 2. Extract SeeAlso links from Wikipedia
```
./bin/flink run -c SeeAlsoExtractor \
    /cpa.jar \
    hdfs:///datasets/enwiki-latest-pages-meta-current.xml \
    hdfs:///wikisim/intermediate/seealso \
    hdfs:///wikisim/intermediate/redirects
```
#### 3. Compute CPA results with alpha={0.5,1.5}
```
./bin/flink run -c WikiSim
    /cpa.jar \
    hdfs:///datasets/enwiki-latest-pages-meta-current.xml \
    hdfs:///wikisim/results/cpa \
    0.5,1.5 0 0 n \
    hdfs:///wikisim/intermediate/redirects
```
#### 4a. SeeAlso Evaluation of CPA results with alpha=1.5
```
./bin/flink run -c SeeAlsoEvaluation \
    /cpa.jar \
    hdfs:///wikisim/results/cpa \
    hdfs:///wikisim/results/evaluation \
    hdfs:///wikisim/intermediate/seealso \
    6
```

#### 4b. ClickStream Evaluation of CPA results with alpha=0.5
```
./bin/flink run -c ClickStreamEvaluation \
    /cpa.jar \
    hdfs:///wikisim/results/cpa \
    hdfs:///datasets/enwiki_2015_02_clickstream.tsv \
    hdfs:///wikisim/results/clickstream_cpa_c \
    5
```

### Evaluation Notes
- When importing CSV outputs to a database (e.g. mysql) use a case sensitive collation (utf8_bin) for Wikipedia article names.


## License

MIT