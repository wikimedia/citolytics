package org.wikipedia.citolytics.edits;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DataSource;
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
 * Offline evaluation of article recommendations based on article edits by Wikipedia users. Authors contribute
 * most likely to article that are related to each other. Therefore, we can utilize edits as gold standard for
 * evaluating recommendations.
 *
 * Approach: Extract article-author pairs, find co-authored articles, order by count.
 *  Result will have article-recommendation-count triples.
 *
 * DataSource: stub-meta-history.xml-dump (available for each language)
 *
 * Problems:
 * - Edits by bots (how to exclude them?, comment contains "bot")
 *
 */
public class EditEvaluation extends WikiSimAbstractJob<RecommendationSet> {
    String inputFilename;

    public static void main(String[] args) throws Exception {
        new EditEvaluation().start(args);
    }

    @Override
    public void plan() throws Exception {
        ParameterTool params = ParameterTool.fromArgs(args);

        inputFilename = params.getRequired("input");
        outputFilename = params.getRequired("output");

        // Read Wikipedia Edit History XML Dump
        DataSource<String> historyDump = env.readFile(new WikiDocumentDelimitedInputFormat(), inputFilename);


        DataSet<ArticleAuthorPair> articleAuthorPairs = historyDump.flatMap(new EditInputMapper());

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

                        out.collect(new AuthorArticlesList(pair.getAuthor(), new ArrayList<>(articles)));
                    }
                });

        DataSet<CoEditList> coEdits = articleAuthorPairs.join(authorArticlesList)
                .where(1) // author id
                .equalTo(0) // author id
                .with(new JoinFunction<ArticleAuthorPair, AuthorArticlesList, CoEditList>() {
                    @Override
                    public CoEditList join(ArticleAuthorPair articleAuthorPair, AuthorArticlesList authorArticlesList) throws Exception {
                        return new CoEditList(articleAuthorPair.getArticle(), authorArticlesList.getArticleList());
                    }
                });

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

                        if(articleName == null)
                            articleName = editList.getArticle();

                        for(String article: editList.getList()) {
                            if(article.equals(articleName)) // Do not recommend the article itself
                                continue;

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

        result = editRecommendations;
    }

}
