package org.wikipedia.citolytics.edits.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.edits.types.ArticleAuthorPair;
import org.wikipedia.citolytics.edits.types.CoEditMap;

import java.util.*;

/**
 * With article filter
 */
public class CoEditsReducer implements GroupReduceFunction<ArticleAuthorPair, CoEditMap> {
    private Collection<String> articleFilter = null;

    public CoEditsReducer(Collection<String> articleFilter) {
        this.articleFilter = articleFilter;
    }

    @Override
    public void reduce(Iterable<ArticleAuthorPair> in, Collector<CoEditMap> out) throws Exception {
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
            for(String article: articles) {
                // Skip if is not part of filter
                if(articleFilter != null && !articleFilter.contains(article))
                    continue;

                // Build co-edit maps
                Map<String, Integer> coArticles = new HashMap<>();
                for(String coArticle: articles) {
                    if(!coArticle.equals(article))
                        coArticles.put(coArticle, 1);
                }

                out.collect(new CoEditMap(article, coArticles));
            }
        }
    }
}