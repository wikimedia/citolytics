package org.wikipedia.citolytics.edits;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.com.google.common.collect.Ordering;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.edits.operators.CoEditsReducer;
import org.wikipedia.citolytics.edits.operators.EditInputMapper;
import org.wikipedia.citolytics.edits.types.ArticleAuthorPair;
import org.wikipedia.citolytics.edits.types.CoEditMap;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResult;
import org.wikipedia.citolytics.seealso.types.WikiSimComparableResultList;

import java.util.*;

/**
 * Extract recommendations based on edit history dumps. See EditEvaluation
 */
public class EditRecommendationExtractor extends WikiSimAbstractJob<RecommendationSet> {

    public static void main(String[] args) throws Exception {
        new EditRecommendationExtractor().start(args);
    }

    @Override
    public void plan() throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        String inputFilename = params.getRequired("input");
        outputFilename = params.getRequired("output");
        String articles = params.get("articles");

        result = extractRecommendations(env, inputFilename, articles != null ? Arrays.asList(articles.split(",")) : null, getParams().has("debug"));
    }

    public static DataSet<RecommendationSet> extractRecommendations(ExecutionEnvironment env, String inputFilename, Collection<String> articleFilter, boolean debug) throws Exception {
        // Read Wikipedia Edit History XML Dump and generate article-author pairs
        DataSet<ArticleAuthorPair> articleAuthorPairs = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename)
                .flatMap(new EditInputMapper())
                .distinct();

        DataSet<CoEditMap> coEdits = articleAuthorPairs
                .groupBy(1) // author id
                .reduceGroup(new CoEditsReducer(articleFilter))
                .groupBy(0)
                .reduce(new ReduceFunction<CoEditMap>() {
                    @Override
                    public CoEditMap reduce(CoEditMap a, CoEditMap b) throws Exception {
                        // Sum number of co-edits
                        Map<String, Integer> coEdits = a.getMap();

                        for(String article: b.getMap().keySet()) {
                            if(coEdits.containsKey(article)) {
                                coEdits.put(article, coEdits.get(article) + b.getMap().get(article));
                            } else {
                                coEdits.put(article, b.getMap().get(article));
                            }
                        }

                        // Reduce size (top-k edits)
//                        if(coEdits.size() > 50) {
//                            coEdits = sortByValue(coEdits, 25);
//                        }

                        return new CoEditMap(a.getArticle(), coEdits);
                    }
                });

        // TODO Remove after debugging
        if(debug) {
            env.fromElements(
                    new Tuple2<String, Long>("__articleAuthorPairs=", articleAuthorPairs.count()),
                    new Tuple2<String, Long>("coEdits=", coEdits.count())
            ).print();
        }

        return coEdits.map(new MapFunction<CoEditMap, RecommendationSet>() {
            /**
             * Build final edit recommendation set (sorted)
             *
             * @param coEdits
             * @return
             * @throws Exception
             */
            @Override
            public RecommendationSet map(CoEditMap coEdits) throws Exception {
                WikiSimComparableResultList<Double> results = new WikiSimComparableResultList<>();

                for (String coEdit : coEdits.getMap().keySet()) {
                    double score = coEdits.getMap().get(coEdit);
                    results.add(new WikiSimComparableResult<>(coEdit, score, 0));
                }

                int topK = 10;
                return new RecommendationSet(
                        coEdits.getArticle(),
                        0, // ignore ids
                        new WikiSimComparableResultList(Ordering.natural().greatestOf(results, topK))
                );
            }
        });
    }

    public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(Map<K, V> map, int topK) {
        List<Map.Entry<K, V>> list =
                new LinkedList<>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
            @Override
            public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
                return (o1.getValue()).compareTo(o2.getValue());
            }
        });

        Map<K, V> result = new LinkedHashMap<>();
        for (Map.Entry<K, V> entry : list) {
            result.put(entry.getKey(), entry.getValue());

            if(result.size() >= topK)
                break;
        }
        return result;
    }
}
