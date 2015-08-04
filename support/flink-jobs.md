Flink Jobs
================================

You run Flink jobs from this repository by using the following commands. Degree of parallelism (-p) depends on cluster setup.

## WikiSim (no redirects)

```
./bin/flink run -c de.tuberlin.dima.schubotz.wikisim.cpa.WikiSim \
    ./cpa.jar \
    hdfs:///datasets/enwiki-latest-pages-meta-current.xml \
    hdfs:///user/mschwarzer/v2/results/a01 \
    0.5,0.8,0.9,1,1.5,2 0 0 n
```

## WikiSim (redirects)

flink run -p 64 -c de.tuberlin.dima.schubotz.wikisim.cpa.WikiSim /home/mschwarzer/wikisim/cpa.jar hdfs:///datasets/enwiki-latest-pages-meta-current.xml hdfs:///user/mschwarzer/v2/results/a01_redirected 0.5,0.8,0.9,1,1.5,2 0 0 n hdfs:///user/mschwarzer/v2/intermediate/redirects


## SeeAlsoEvaluation

### CPA
flink run -p 64 -c de.tuberlin.dima.schubotz.wikisim.seealso.SeeAlsoEvaluation /home/mschwarzer/wikisim/cpa.jar hdfs:///user/mschwarzer/v2/results/a01_redirected/1 hdfs:///user/mschwarzer/v2/results/seealso_cpa_1_0 hdfs:///user/mschwarzer/v2/intermediate/seealso_redirects nofilter 8

### MLT
flink run -p 64 -c de.tuberlin.dima.schubotz.wikisim.seealso.SeeAlsoEvaluation /home/mschwarzer/wikisim/cpa.jar hdfs:///user/mschwarzer/v2/intermediate/mlt_results hdfs:///user/mschwarzer/v2/results/seealso_mlt hdfs:///user/mschwarzer/v2/intermediate/seealso_redirects nofilter 0 0 0 y


## ClickStreamEvaluation



## Resolve Redirects (from WikiSim output)

flink run -p 96 -c de.tuberlin.dima.schubotz.wikisim.redirects.single.WikiSimRedirects /home/mschwarzer/wikisim/cpa.jar hdfs:///user/mschwarzer/v2/results/a01 hdfs:///user/mschwarzer/v2/intermediate/redirects hdfs:///user/mschwarzer/v2/results/a01_redirected

## Extract Redirects (from WikiDump)

```
./bin/flink run -c de.tuberlin.dima.schubotz.wikisim.redirects.RedirectExtractor \
    /home/mschwarzer/wikisim/cpa.jar \
    hdfs:///datasets/enwiki-latest-pages-meta-current.xml \
    hdfs:///user/mschwarzer/v2/intermediate/redirects2 \
```

## Replace redirects in "See Also" links

flink run -p 64 -c de.tuberlin.dima.schubotz.wikisim.redirects.SeeAlsoRedirects /home/mschwarzer/wikisim/cpa.jar hdfs:///user/mschwarzer/v2/intermediate/seealso6e.csv hdfs:///user/mschwarzer/v2/intermediate/redirects hdfs:///user/mschwarzer/v2/intermediate/seealso_redirects