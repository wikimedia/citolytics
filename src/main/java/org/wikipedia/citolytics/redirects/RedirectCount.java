package org.wikipedia.citolytics.redirects;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple1;
import org.wikipedia.citolytics.WikiSimAbstractJob;

import java.util.regex.Pattern;

/**
 * Count redirects in redirects.out (RedirectExtractor)
 */
public class RedirectCount extends WikiSimAbstractJob<Tuple1<Integer>> {
    public static void main(String[] args) throws Exception {
        new RedirectCount()
                .enableSingleOutputFile()
                .enableTextOutput()
                .start(args);
    }

    public void plan() {

        outputFilename = args[2];

        // article|link target
        DataSet<Tuple1<String>> links = env.readTextFile(args[0])
                .map(new MapFunction<String, Tuple1<String>>() {
                    @Override
                    public Tuple1<String> map(String s) throws Exception {
                        String[] cols = s.split(Pattern.quote("|"));
                        return new Tuple1<>(cols[1]);
                    }
                });

        // source|redirect target
        DataSet<Tuple1<String>> redirects = env.readTextFile(args[1])
                .map(new MapFunction<String, Tuple1<String>>() {
                    @Override
                    public Tuple1<String> map(String s) throws Exception {
                        String[] cols = s.split(Pattern.quote("|"));
                        return new Tuple1<>(cols[0]);
                    }
                });

        result = links.join(redirects)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple1<String>, Tuple1<String>, Tuple1<Integer>>() {
                    @Override
                    public Tuple1<Integer> join(Tuple1<String> a, Tuple1<String> b) throws Exception {
                        return new Tuple1<>(1);
                    }
                })
                .sum(0);
    }
}
