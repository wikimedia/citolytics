package org.wikipedia.citolytics.stats;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.util.ListCollector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.io.WikiOutputFormat;
import org.wikipedia.citolytics.cpa.operators.RecommendationPairExtractor;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.stats.utils.ArticleFilter;
import org.wikipedia.citolytics.stats.utils.RandomSampler;
import org.wikipedia.processing.DocumentProcessor;
import org.wikipedia.processing.types.WikiDocument;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.wikipedia.citolytics.stats.CPISampler.roundToDecimals;

/**
 * Extracts a sample of link distances
 * - absolute distance (in words)
 * - relative distance
 * - structure distance
 *
 */
public class LinkDistanceSampler extends WikiSimAbstractJob<Tuple> {

    public final static String ABSOLUTE_DISTANCE = "abs";
    public final static String RELATIVE_DISTANCE = "rel";
    public final static String STRUCTURE_DISTANCE = "str";
    public final static int DECIMALS = 5;


    public static void main(String[] args) throws Exception {
        new LinkDistanceSampler().start(args);
    }


    @Override
    public void plan() throws Exception {
        this.disableOutput = true;
        this.outputFilename = getParams().getRequired("output");

        String inputPath = getParams().getRequired("input");
        double sampleRate = getParams().getDouble("p", 0.1);

        DataSet<Tuple2<String, Double>> allTypes = env.readFile(new WikiDocumentDelimitedInputFormat(), inputPath)
                .filter(new RandomSampler<String>(sampleRate))
                .flatMap(new FlatMapFunction<String, Tuple2<String, Double>>() {
            @Override
            public void flatMap(String content, Collector<Tuple2<String, Double>> out) throws Exception {

                WikiDocument doc = new DocumentProcessor().processDoc(content);
                if (doc == null) return;

                // Absolute distance
                RecommendationPairExtractor absExtractor = new RecommendationPairExtractor();
                absExtractor.setAlphas(new double[]{-1.0});

                List<RecommendationPair> absPairs = new ArrayList<>();
                absExtractor.collectLinkPairs(doc, new ListCollector<>(absPairs));

                for(RecommendationPair rec: absPairs) {
                    out.collect(new Tuple2<>(ABSOLUTE_DISTANCE, roundToDecimals(rec.getCPI(0), DECIMALS)));
                }

                // Relative distance
                RecommendationPairExtractor relExtractor = new RecommendationPairExtractor();
                relExtractor.enableRelativeProximity();
                relExtractor.setAlphas(new double[]{-1.0});

                List<RecommendationPair> relPairs = new ArrayList<>();
                relExtractor.collectLinkPairs(doc, new ListCollector<>(relPairs));

                for(RecommendationPair rec: relPairs) {
                    out.collect(new Tuple2<>(RELATIVE_DISTANCE, roundToDecimals(rec.getCPI(0), DECIMALS)));
                }

                // Structure distance
                List<RecommendationPair> strPairs = new ArrayList<>();
                new RecommendationPairExtractor().collectLinkPairsBasedOnStructure(doc, new ListCollector<>(strPairs));

                for(RecommendationPair rec: strPairs) {
                    out.collect(new Tuple2<>(STRUCTURE_DISTANCE, roundToDecimals(rec.getCPI(0), DECIMALS)));
                }

            }

        });

        writeByType(ABSOLUTE_DISTANCE, allTypes, outputFilename);
        writeByType(RELATIVE_DISTANCE, allTypes, outputFilename);
        writeByType(STRUCTURE_DISTANCE, allTypes, outputFilename);

        env.execute();
    }

    private void writeByType(String type, DataSet<Tuple2<String, Double>> allItems, String outputPath) throws Exception {
        DataSet<Tuple2<Double, Integer>> typeItems = allItems
            .filter(new ArticleFilter<>(Arrays.asList(type), 0))
            .map(new LinkDistanceMapper())
            .groupBy(0)
            .sum(1);

        typeItems
            .write(new WikiOutputFormat<>(outputPath + "." + type), outputPath + "." + type, FileSystem.WriteMode.OVERWRITE)
            .setParallelism(1);

//        System.out.println("foo" + typeItems.count());
    }

    public class LinkDistanceMapper implements MapFunction<Tuple2<String, Double>, Tuple2<Double, Integer>> {
        @Override
        public Tuple2<Double, Integer> map(Tuple2<String, Double> in) throws Exception {
            return new Tuple2<>(in.f1, 1);
        }
    }

    public class LinkDistanceCollector implements Collector<RecommendationPair> {
        private Collector<Tuple2<String, Double>> out;
        private String type;

        public LinkDistanceCollector(String type, Collector<Tuple2<String, Double>> out) {
            this.type = type;
            this.out = out;
        }

        @Override
        public void collect(RecommendationPair rec) {
            out.collect(new Tuple2<>(type, rec.getCPI(0)));
        }

        @Override
        public void close() {

        }
    }
}
