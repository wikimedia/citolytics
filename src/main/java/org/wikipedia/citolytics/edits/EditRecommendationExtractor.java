package org.wikipedia.citolytics.edits;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.com.google.common.collect.Ordering;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.io.WikiDocumentDelimitedInputFormat;
import org.wikipedia.citolytics.cpa.types.RecommendationSet;
import org.wikipedia.citolytics.edits.operators.EditInputMapper;
import org.wikipedia.citolytics.edits.types.ArticleAuthorPair;
import org.wikipedia.citolytics.edits.types.AuthorArticlesList;
import org.wikipedia.citolytics.edits.types.CoEditList;
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

        result = extractRecommendations(env, inputFilename);
    }

    public static DataSet<RecommendationSet> extractRecommendations(ExecutionEnvironment env, String inputFilename) {
        // Read Wikipedia Edit History XML Dump and generate article-author pairs
        DataSet<ArticleAuthorPair> articleAuthorPairs = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename)
                .flatMap(new EditInputMapper())
                .distinct();

        DataSet<AuthorArticlesList> authorArticlesList = articleAuthorPairs
                .groupBy(1) // author id
                .reduceGroup(new GroupReduceFunction<ArticleAuthorPair, AuthorArticlesList>() {
                    @Override
                    public void reduce(Iterable<ArticleAuthorPair> in, Collector<AuthorArticlesList> out) throws Exception {
                        Iterator<ArticleAuthorPair> iterator = in.iterator();
                        HashSet<String> articles = new HashSet<>();
                        ArticleAuthorPair pair = null;

                        while (iterator.hasNext()) {
                            pair = iterator.next();
                            articles.add(pair.getArticle());
                        }

                        // Only author with at least two articles are useful
                        if (articles.size() > 1) {
//                            System.out.println(pair.getAuthor() + " // " + articles);
                            out.collect(new AuthorArticlesList(pair.getAuthor(), new ArrayList<>(articles)));
                        }
                    }
                });

        // Test1 seems slower
//        return test1(articleAuthorPairs, authorArticlesList);
        return test2(articleAuthorPairs, authorArticlesList);

//        articleAuthorPairs.print();
//        System.out.println("---");
//        authorArticlesList.print();
    }

    public static DataSet<RecommendationSet> test1(DataSet<ArticleAuthorPair> articleAuthorPairs, DataSet<AuthorArticlesList> authorArticlesList) {
        DataSet<RecommendationSet> tmp = articleAuthorPairs.join(authorArticlesList, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
                .where(1) // author id
                .equalTo(0) // author id
                .with(new JoinFunction<ArticleAuthorPair, AuthorArticlesList, CoEditMap>() {
                    @Override
                    public CoEditMap join(ArticleAuthorPair articleAuthorPair, AuthorArticlesList authorArticlesList) throws Exception {
//                        WikiSimComparableResultList<Double> list = new WikiSimComparableResultList<>();
                        Map<String, Integer> map = new HashMap<>();

                        for (String article : authorArticlesList.getArticleList()) {
                            if (article.equals(articleAuthorPair.getArticle())) // Do not recommend the article itself
                                continue;

                            // ignore article id
//                            list.add(new WikiSimComparableResult<>(article, 1., 0));

                            map.put(article, 1);
                        }

//                         return new RecommendationSet(articleAuthorPair.getArticle(), 0, list);
                        return new CoEditMap(articleAuthorPair.getArticle(), map);
                    }
                })
                .groupBy(0) // article
                .reduce(new ReduceFunction<CoEditMap>() {
                    @Override
                    public CoEditMap reduce(CoEditMap a, CoEditMap b) throws Exception {
//                        String article = a.getArticle();
                        for (String coEdit : b.getMap().keySet()) {
                            if (a.getMap().containsKey(coEdit)) {
//                                Integer count = a.getMap().get(coEdit);
//                                count += b.getMap().get(coEdit);
                                a.getMap().put(coEdit, a.getMap().get(coEdit) + b.getMap().get(coEdit));
                            } else {
                                a.getMap().put(coEdit, b.getMap().get(coEdit));
                            }
                        }
                        return a;
                    }
                })
                .map(new MapFunction<CoEditMap, RecommendationSet>() {
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
        return tmp;
    }


    public static DataSet<RecommendationSet> test2(DataSet<ArticleAuthorPair> articleAuthorPairs, DataSet<AuthorArticlesList> authorArticlesList) {
        articleAuthorPairs.join(authorArticlesList, JoinOperatorBase.JoinHint.BROADCAST_HASH_SECOND)
                .where(1) // author id
                .equalTo(0) // author id
                .with(new JoinFunction<ArticleAuthorPair, AuthorArticlesList, RecommendationSet>() {
                    @Override
                    public RecommendationSet join(ArticleAuthorPair articleAuthorPair, AuthorArticlesList authorArticlesList) throws Exception {
                        WikiSimComparableResultList<Double> list = new WikiSimComparableResultList<>();

                        for (String article : authorArticlesList.getArticleList()) {
                            if (article.equals(articleAuthorPair.getArticle())) // Do not recommend the article itself
                                continue;

                            // ignore article id
                            list.add(new WikiSimComparableResult<>(article, 1., 0));
                        }

                         return new RecommendationSet(articleAuthorPair.getArticle(), 0, list);
                    }
                });

//
//                .reduce(new ReduceFunction<RecommendationSet>() {
//                    @Override
//                    public RecommendationSet reduce(RecommendationSet a, RecommendationSet b) throws Exception {
//                        String article = a.getSourceTitle();
//                        Map<String, Integer> coEditedArticles = new HashMap<>();
//
//                        for(WikiSimComparableResult<Double> coEdit: a.getResults()) {
//
//                        }
//
//                        WikiSimComparableResultList<Double> results = a.getResults();
//                        results.addAll(b.getResults());
//
//                        int topK = 10;
//
//                        return new RecommendationSet(
//                                article,
//                                0, // ignore ids
//                                new WikiSimComparableResultList(Ordering.natural().greatestOf(results, topK))
//                        );
//                    }
//                });
//        return tmp;

        DataSet<CoEditList> coEdits = articleAuthorPairs.join(authorArticlesList)
                .where(1) // author id
                .equalTo(0) // author id
                .with(new JoinFunction<ArticleAuthorPair, AuthorArticlesList, CoEditList>() {
                    @Override
                    public CoEditList join(ArticleAuthorPair articleAuthorPair, AuthorArticlesList authorArticlesList) throws Exception {

                        return new CoEditList(articleAuthorPair.getArticle(), authorArticlesList.getArticleList());
                    }
                });

//        // TODO rewrite as reduce
        DataSet<RecommendationSet> editRecommendations = coEdits
            .groupBy(0) // article
            .reduceGroup(new GroupReduceFunction<CoEditList, RecommendationSet>() {
                @Override
                public void reduce(Iterable<CoEditList> edits, Collector<RecommendationSet> out) throws Exception {
                    Iterator<CoEditList> iterator = edits.iterator();
                    CoEditList editList = null;
                    String articleName = null;
                    Map<String, Integer> coEditedArticles = new HashMap<>();
                    List<WikiSimComparableResult<Double>> recommendations = new ArrayList<>();

                    // Build recommendation set (count co-edits per article)
                    while(iterator.hasNext()) {
                        editList = iterator.next();

                        if(articleName == null) {
                            articleName = editList.getArticle();
//                            System.out.println("## article = " + articleName);
                        }

                        for(String article: editList.getList()) {
                            if(article.equals(articleName)) // Do not recommend the article itself
                                continue;

//                            System.out.println("-- " + article);

                            if(coEditedArticles.containsKey(article)) {
                                coEditedArticles.put(article, coEditedArticles.get(article) + 1);
                            } else {
                                coEditedArticles.put(article, 1);
                            }
                        }
                    }

                    for(String article: coEditedArticles.keySet()) {
                        double score = coEditedArticles.get(article);
                        int articleId = 0; // ignore ids
                        recommendations.add(new WikiSimComparableResult<>(article, score, articleId));
                    }

                    // Order recommendations
                    int topK = 10;
                    List<WikiSimComparableResult<Double>> orderedRecommendations = Ordering.natural().greatestOf(recommendations, topK);

                    out.collect(new RecommendationSet(
                            editList.getArticle(),
                            0, // ignore ids
                            new WikiSimComparableResultList(orderedRecommendations)
                    ));
                }
            });

//        coEdits.print();
//        editRecommendations.print();

        return editRecommendations;
//        return tmp;
    }

}
