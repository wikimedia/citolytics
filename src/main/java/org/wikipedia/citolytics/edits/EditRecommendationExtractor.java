package org.wikipedia.citolytics.edits;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
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

                        while(iterator.hasNext()) {
                            pair = iterator.next();
                            articles.add(pair.getArticle());
                        }

                        // Only author with at least two articles are useful
                        if(articles.size() > 1) {
//                            System.out.println(pair.getAuthor() + " // " + articles);
                            out.collect(new AuthorArticlesList(pair.getAuthor(), new ArrayList<>(articles)));
                        }
                    }
                });

//        articleAuthorPairs.print();
//        System.out.println("---");
//        authorArticlesList.print();

        DataSet<CoEditList> coEdits = articleAuthorPairs.join(authorArticlesList)
                .where(1) // author id
                .equalTo(0) // author id
                .with(new JoinFunction<ArticleAuthorPair, AuthorArticlesList, CoEditList>() {
                    @Override
                    public CoEditList join(ArticleAuthorPair articleAuthorPair, AuthorArticlesList authorArticlesList) throws Exception {

//                        System.out.println("join: " + articleAuthorPair.getArticle() + " >> " + authorArticlesList.getArticleList());

                        return new CoEditList(articleAuthorPair.getArticle(), authorArticlesList.getArticleList());
//                        List<WikiSimComparableResult<Double>> list = new ArrayList<>();
//
//                        for(String article: authorArticlesList.getArticleList()) {
                             // ignore article id
//                            list.add(new WikiSimComparableResult<Double>(article, 1., 0));
//                        }

//                        list = Ordering.natural().greatestOf(list, 10);

//                        return new CoEditList(articleAuthorPair.getArticle(), list);
                        // return new RecommendationSet()...
                    }
                });

        // TODO rewrite as reduce
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
    }

}
