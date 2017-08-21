package org.wikipedia.citolytics.edits.operators;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.edits.types.ArticleAuthorPair;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * DataSource format for Wikipedia stub-meta-history.xml dumps
 */
public class EditInputMapper implements FlatMapFunction<String, ArticleAuthorPair> {
    @Override
    public void flatMap(String xml, Collector<ArticleAuthorPair> out) throws Exception {
        // Extract title
        Pattern titleRegex = Pattern.compile("<title>(.*?)</title>", Pattern.DOTALL);
        Pattern revRegex = Pattern.compile("<revision>(.*?)</revision>", Pattern.DOTALL);
        Pattern commentRegex = Pattern.compile("<comment>(.*?)</comment>", Pattern.DOTALL);
        Pattern contributorRegex = Pattern.compile("<contributor>(\\s+)<username>(.*?)</username>(\\s+)<id>(.*?)</id>(\\s+)</contributor>", Pattern.DOTALL);

        Matcher titleMatcher = titleRegex.matcher(xml);

        if (!titleMatcher.find()) {
            // no title found
            return;
        }

        String title = titleMatcher.group(1);

        if(title.contains(":")) {
            // Only article from main namespace
            return;
        }

//        System.out.println("--" + title);

        Matcher revMatcher = revRegex.matcher(xml);
        while (revMatcher.find()) {
            Matcher contributorMatcher = contributorRegex.matcher(revMatcher.group(1));
            Matcher commentMatcher = commentRegex.matcher(revMatcher.group(1));

            String comment = commentMatcher.find() ? commentMatcher.group(1) : "";

            if (comment.contains("bot") || comment.contains("Bot") || comment.contains("BOT")) {
                // Exclude bot edits
                return;
            }

            if (contributorMatcher.find()) {
                // Use name or id?
                String authorName = contributorMatcher.group(2);
                int authorId = Integer.valueOf(contributorMatcher.group(4));

                if (authorId > 0) {
                    // Exclude guest users
                    out.collect(new ArticleAuthorPair(title, authorId));
                }
            }
        }
    }
}
