# Co-Citation Proximity Analysis (CPA) for Wikipedia

[![Build Status](https://drone.io/github.com/TU-Berlin/cpa-demo/status.png)](https://drone.io/github.com/TU-Berlin/cpa-demo/latest)
### Contents
* CPA and Co-Citation computation on Wikipedia text corpus
* Extraction of "See also" link as gold standard
* Performance evaluation with Mean Average Precision, TopK, ... 
* LinkGraph extraction for result pairs

### Run
* Compile the Maven project
* Run Apache Flink jobs separately from jar-file with -c parameter.

### Available Flink Jobs (Classes)
* **CPA/CoCit Computation**: de.tuberlin.dima.schubotz.wikisim.cpa.WikiSim
    * Parameters: <DATASET> <OUTPUT> <ALPHA1, ALPHA2, ...> [REDUCER-THRESHOLD] [COMBINER-THRESHOLD] []
    * e.g.: hdfs://cluster/wikidump.xml hdfs://cluster/results.out 0.5,1.0,1.5,2.0 10 5

* **SeeAlsoEvaluation**: de.tuberlin.dima.schubotz.wikisim.seealso.SeeAlsoEvaluation
    * Parameters: [OUTPUT] [SEEALSO-DATASET] [WIKISIM-DATASET] [MLT-DATASET] [LINKS/NOFILTER] [TOP-K1, TOP-K2, ...] [CPA-FIELD]
    * e.g.: hdfs://cluster/eval.out hdfs://cluster/seealso.csv hdfs://cluster/results.out hdfs://cluster/mlt.csv nofilter 10,5,1 8

* **SeeAlsoExtractor**: de.tuberlin.dima.schubotz.wikisim.seealso.SeeAlsoExtractor
    * Parameters: -
    * e.g.: -
    
    
* **Extract Redirects**: de.tuberlin.dima.schubotz.wikisim.redirects.RedirectExtractor
    * Parameters: -
    * e.g.: -
    
* **ClickStreamEvaluation**: de.tuberlin.dima.schubotz.wikisim.clickstream.ClickStreamEvaluation
    * Parameters: -
    * e.g.: -
    

...

### Evaluation Example
To evaluate and generate the data used in our publication, you need to run the following Flink jobs in the order below. Paths to JAR and HDFS need to be adjusted depending on your setup. 

#### 1. Extract Redirects from Wikipedia
```
./bin/flink run -c de.tuberlin.dima.schubotz.wikisim.redirects.RedirectExtractor \
    /cpa.jar \
    hdfs:///datasets/enwiki-latest-pages-meta-current.xml \
    hdfs:///wikisim/intermediate/redirects \
```

#### 2. Extract SeeAlso links from Wikipedia
```
./bin/flink run -c de.tuberlin.dima.schubotz.wikisim.seealso.SeeAlsoExtractor \
    /cpa.jar \
    hdfs:///datasets/enwiki-latest-pages-meta-current.xml \
    hdfs:///wikisim/intermediate/seealso \
    hdfs:///wikisim/intermediate/redirects
```
#### 3. Compute CPA results with alpha={0.5,1.5}
```
./bin/flink run -c de.tuberlin.dima.schubotz.wikisim.cpa.WikiSim
    /cpa.jar \
    hdfs:///datasets/enwiki-latest-pages-meta-current.xml \
    hdfs:///wikisim/results/cpa \
    0.5,1.5 0 0 n \
    hdfs:///wikisim/intermediate/redirects
```
#### 4. SeeAlso Evaluation of CPA results with alpha=1.5
```
./bin/flink run -c de.tuberlin.dima.schubotz.wikisim.seealso.SeeAlsoEvaluation
    /cpa.jar \
    hdfs:///wikisim/results/cpa \
    hdfs:///wikisim/results/evaluation \
    hdfs:///wikisim/intermediate/seealso \
    6
``