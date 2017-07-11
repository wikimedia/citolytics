package org.wikipedia.citolytics.stats;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.clickstream.types.ClickStreamTuple;
import org.wikipedia.citolytics.clickstream.utils.ClickStreamHelper;
import org.wikipedia.citolytics.cpa.io.WikiOutputFormat;
import org.wikipedia.citolytics.cpa.io.WikiSimReader;
import org.wikipedia.citolytics.cpa.types.Recommendation;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;

import java.util.*;

/**
 * Extract data that can be used for detailed analysis of CPI values specific articles.
 *
 * Usage:
 *  --top-k 3 --articles "Albert Einstein,Cheese" --wikisim hdfs://... --clickstreams hdfs://.. --stats hdfs://..
 *      --score 5 --output print
 *
 * Input:
 * - int: top-k
 * - str: Article titles (comma separated)
 * - dataset: WikiSim raw
 * - dataset: ClickStream
 * - dataset: SeeAlso
 * - dataset: Stats
 *
 * Output:
 * - WikiSim: Source | Recommendation | CPI | Recommendation in-links | Clicks | (is top-click) | (is see also)
 * - ClickStream: Source | Target | Click |
 * - See also: Source | Target
 * -
 */
public class CPIAnalysis extends WikiSimAbstractJob<Tuple5<String, String, Double, Integer, Integer>> {

    public static void main(String[] args) throws Exception {
        new CPIAnalysis().start(args);
    }

    @Override
    public void plan() throws Exception {
        enableSingleOutputFile();

        outputFilename = getParams().getRequired("output");
        int topK = getParams().getInt("top-k", WikiSimConfiguration.DEFAULT_TOP_K);
        Set<String> articles = new HashSet<>(Arrays.asList(getParams().getRequired("articles").split(",")));

        String wikiSimPath = getParams().getRequired("wikisim");
        String clickStreamPath = getParams().getRequired("clickstream");
        String clickStreamOutputPath = getParams().getRequired("clickstream-output");

        String statsPath = getParams().get("stats");
        int fieldScore = getParams().getInt("score", WikiSimConfiguration.DEFAULT_FIELD_SCORE);
        int fieldPageA = getParams().getInt("page-a", WikiSimConfiguration.DEFAULT_FIELD_PAGE_A);
        int fieldPageB = getParams().getInt("page-b", WikiSimConfiguration.DEFAULT_FIELD_PAGE_B);
        int fieldPageIdA = getParams().getInt("page-id-a", RecommendationPair.PAGE_A_ID_KEY);
        int fieldPageIdB = getParams().getInt("page-id-b", RecommendationPair.PAGE_B_ID_KEY);

        String lang = getParams().get("lang");
        String langLinksPath = getParams().get("lang-links");
        String idTitleMappingPath = getParams().get("id-title-mapping");

        // Read recommendations and filter by source articles
        DataSet<Recommendation> recommendations = WikiSimReader.readWikiSimOutput(env, wikiSimPath, fieldPageA, fieldPageB, fieldScore, fieldPageIdA, fieldPageIdB)
                .filter(new ArticleFilter<>(articles, Recommendation.SOURCE_TITLE_KEY));

        // Stats do not need to be filtered
        DataSet<ArticleStatsTuple> stats = ArticleStats.getArticleStatsFromFile(env, statsPath);

        DataSet<ClickStreamTuple> clickStreams =
                ClickStreamHelper.getTranslatedClickStreamDataSet(env, clickStreamPath, lang,
                        langLinksPath, idTitleMappingPath)
                .filter(new ArticleFilter<>(articles, ClickStreamTuple.ARTICLE_NAME_KEY));

        // Write filtered click streams
        if(clickStreamOutputPath != null) {
            clickStreams
                    .flatMap(new FlatMapFunction<ClickStreamTuple, Tuple3<String, String, Integer>>() {
                        @Override
                        public void flatMap(ClickStreamTuple in, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                            for(Map.Entry<String, Integer> click: in.getOutClicks().entrySet()) {
                                out.collect(new Tuple3<>(in.getArticleName(), click.getKey(), click.getValue()));
                            }
                        }
                    })
                    .write(new WikiOutputFormat<>(clickStreamOutputPath), clickStreamOutputPath, FileSystem.WriteMode.OVERWRITE)
                    .setParallelism(1);
        }

        // Annotate recommendations with clicks and in-links
        result = recommendations
                .leftOuterJoin(stats)
                .where(Recommendation.RECOMMENDATION_TITLE_KEY)
                .equalTo(ArticleStatsTuple.ARTICLE_NAME_KEY)
                .with(new InLinksAnnotator())
                // Add clicks
                .coGroup(clickStreams)
                .where(0)
                .equalTo(ClickStreamTuple.ARTICLE_NAME_KEY)
                .with(new ClicksAnnotator())
                // TODO Add See also
        ;

    }

    class ClicksAnnotator implements CoGroupFunction<Tuple5<String, String, Double, Integer, Integer>, ClickStreamTuple, Tuple5<String, String, Double, Integer, Integer>> {
        @Override
        public void coGroup(Iterable<Tuple5<String, String, Double, Integer, Integer>> iterable, Iterable<ClickStreamTuple> iterableClickStreams, Collector<Tuple5<String, String, Double, Integer, Integer>> out) throws Exception {
            Iterator<ClickStreamTuple> iteratorClickStream = iterableClickStreams.iterator();
            Iterator<Tuple5<String, String, Double, Integer, Integer>> iteratorRecs = iterable.iterator();
            Map<String, Integer> clicks = new HashMap<>();

            // Fetch clicks from clickstream
            if(iteratorClickStream.hasNext()) {
                clicks = iteratorClickStream.next().getOutClicks();
            }

            if(iteratorClickStream.hasNext()) {
                throw new Exception("Multiple clickstream tuples where there should be only a single one.");
            }

            // Collect all recommendations
            while(iteratorRecs.hasNext()) {
                Tuple5<String, String, Double, Integer, Integer> rec = iteratorRecs.next();

                // Clicks for recommendation
                if(clicks.containsKey(rec.f1))
                    rec.f4 = clicks.get(rec.f1);

                out.collect(rec);
            }
        }
    }

    class InLinksAnnotator implements JoinFunction<Recommendation, ArticleStatsTuple, Tuple5<String, String, Double, Integer, Integer>> {
        @Override
        public Tuple5<String, String, Double, Integer, Integer> join(Recommendation recommendation, ArticleStatsTuple stats) throws Exception {
            int inLinks = 0;

            if(stats != null) {
                inLinks = stats.getInLinks();
            }

            return new Tuple5<>(
                    recommendation.getSourceTitle(),
                    recommendation.getRecommendationTitle(),
                    recommendation.getScore(),
                    inLinks,
                    0
            );
        }
    }

    class ArticleFilter<T extends Tuple> implements FilterFunction<T> {
        private Set<String> articles;
        private int field;

        ArticleFilter(Set<String> articles, int field) {
            this.articles = articles;
            this.field = field;
        }

        @Override
        public boolean filter(T recommendation) throws Exception {
            return articles.contains(recommendation.getField(field));
        }
    }
}
