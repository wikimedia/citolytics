
WikiSim (no redirects)

flink run -p 64 -c de.tuberlin.dima.schubotz.wikisim.cpa.WikiSim /home/mschwarzer/wikisim/cpa.jar hdfs:///datasets/enwiki-latest-pages-meta-current.xml hdfs:///user/mschwarzer/v2/results/a01 0.5,0.8,0.9,1,1.5,2 0 0 n


WikiSim (redirects)

flink run -p 64 -c de.tuberlin.dima.schubotz.wikisim.cpa.WikiSim /home/mschwarzer/wikisim/cpa.jar hdfs:///datasets/enwiki-latest-pages-meta-current.xml hdfs:///user/mschwarzer/v2/results/a01_redirected 0.5,0.8,0.9,1,1.5,2 0 0 n hdfs:///user/mschwarzer/v2/intermediate/redirects


ClickStreamEvaluation

Redirects (from WikiSim output)

flink run -p 96 -c de.tuberlin.dima.schubotz.wikisim.redirects.WikiSimRedirects /home/mschwarzer/wikisim/cpa.jar hdfs:///user/mschwarzer/v2/results/a01 hdfs:///user/mschwarzer/v2/intermediate/redirects hdfs:///user/mschwarzer/v2/results/a01_redirected