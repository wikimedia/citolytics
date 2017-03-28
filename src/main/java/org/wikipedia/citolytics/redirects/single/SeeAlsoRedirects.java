package org.wikipedia.citolytics.redirects.single;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;

public class SeeAlsoRedirects extends WikiSimAbstractJob<Tuple3<String, String, Integer>> {
    public static void main(String[] args) throws Exception {
        new SeeAlsoRedirects().start(args);
    }

    public void plan() {
        if (args.length <= 2) {
            System.err.print("Error: Parameter missing! Required: <SEEALSO> <REDIRECTS> <OUTPUT>, Current: " + Arrays.toString(args));
            System.exit(1);
        }

        outputFilename = args[2];

        int pageA = 1;
        int pageB = 2;
        int redirectSource = 0;
        int redirectTarget = 1;

        DataSet<RedirectMapping> redirects = WikiSimRedirects.getRedirectsDataSet(env, args[1]);

        result = env.readTextFile(args[0])
                // read and map to tuple2 structure
                .flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple2<String, String>> out) throws Exception {
                        String[] cols = s.split(Pattern.quote("|"));
                        if (cols.length >= 2) {
                            String[] links = cols[1].split(Pattern.quote("#"));

                            for (String link : links) {
                                out.collect(new Tuple2<>(
                                        cols[0],
                                        link
                                ));
                            }
                        } else {
                            throw new Exception("Cannot read See also input: " + s);
                        }
                    }
                })
                .coGroup(redirects)
                .where(1) // see also link
                .equalTo(0) // redirect source
                        // replace
                .with(new CoGroupFunction<Tuple2<String, String>, RedirectMapping, Tuple2<String, String>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> seeAlso, Iterable<RedirectMapping> redirect, Collector<Tuple2<String, String>> out) throws Exception {
                        Iterator<Tuple2<String, String>> iteratorSeeAlso = seeAlso.iterator();
                        Iterator<RedirectMapping> iteratorRedirect = redirect.iterator();

                        while (iteratorSeeAlso.hasNext()) {
                            Tuple2<String, String> recordSeeAlso = iteratorSeeAlso.next();

                            if (iteratorRedirect.hasNext()) {
                                RedirectMapping recordRedirect = iteratorRedirect.next();

                                // replace
                                recordSeeAlso.setField(recordRedirect.getTarget(), 1);
                            }
                            out.collect(recordSeeAlso);
                        }
                    }
                })
                        // group back to see also structure
                .groupBy(0)
                .reduceGroup(new GroupReduceFunction<Tuple2<String, String>, Tuple3<String, String, Integer>>() {
                    @Override
                    public void reduce(Iterable<Tuple2<String, String>> in, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        Iterator<Tuple2<String, String>> iterator = in.iterator();
                        String article = null;
                        String seeAlsoLinks = null;
                        int counter = 0;

                        while (iterator.hasNext()) {
                            Tuple2<String, String> link = iterator.next();
                            if (article == null) {
                                article = link.f0;
                                seeAlsoLinks = link.f1;
                            } else {
                                seeAlsoLinks += "#" + link.f1;
                            }
                            counter++;
                        }

                        out.collect(new Tuple3<>(article, seeAlsoLinks, counter));
                    }
                });
    }
}
