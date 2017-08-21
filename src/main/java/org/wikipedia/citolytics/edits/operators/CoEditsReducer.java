package org.wikipedia.citolytics.edits.operators;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.edits.types.ArticleAuthorPair;
import org.wikipedia.citolytics.edits.types.CoEditMap;

import java.util.*;

/**
 * Take article-author-pairs and generate co-edit pairs.
 * (With article filter)
 */
public class CoEditsReducer implements GroupReduceFunction<ArticleAuthorPair, CoEditMap> {
    private Collection<String> articleFilter;

    public CoEditsReducer(Collection<String> articleFilter) {
        this.articleFilter = articleFilter;
    }

    @Override
    public void reduce(Iterable<ArticleAuthorPair> in, Collector<CoEditMap> out) throws Exception {
        Iterator<ArticleAuthorPair> iterator = in.iterator();
        HashSet<String> articles = new HashSet<>();
        ArticleAuthorPair pair;

        // Read all edited articles from this author
        while (iterator.hasNext()) {
            pair = iterator.next();
            articles.add(pair.getArticle());
        }

        // Only author with at least two articles are useful
        if (articles.size() > 1) {
//                            System.out.println(pair.getAuthorId() + " // " + articles);
            // Loop over all articles twice (generate edit pairs)
            for(String article: articles) {
                // Skip if is not part of filter
                if(articleFilter != null && !articleFilter.contains(article))
                    continue;

                // Build co-edit maps
                Map<String, Integer> coArticles = new HashMap<>();
                for(String coArticle: articles) {
                    if(!coArticle.equals(article)) // No a-a pairs
                        coArticles.put(coArticle, 1);
                }

                out.collect(new CoEditMap(article, coArticles));
            }
        }
    }
}