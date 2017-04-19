package org.wikipedia.citolytics.redirects.single;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.WikiSimAbstractJob;
import org.wikipedia.citolytics.cpa.types.LinkPair;
import org.wikipedia.citolytics.cpa.types.RedirectMapping;
import org.wikipedia.citolytics.seealso.SeeAlsoExtractor;

import java.util.Iterator;
import java.util.regex.Pattern;

/**
 * Resolve redirects in "See also" links
 */
public class SeeAlsoRedirects extends WikiSimAbstractJob<Tuple3<String, String, Integer>> {
    public static void main(String[] args) throws Exception {
        new SeeAlsoRedirects().start(args);
    }

    public void plan() {
        ParameterTool params = ParameterTool.fromArgs(args);

        outputFilename = params.getRequired("output");

        DataSet<RedirectMapping> redirects = WikiSimRedirects.getRedirectsDataSet(env, params.getRequired("redirects"));

        result = resolveRedirects(env.readTextFile(params.getRequired("seealso"))
                // read and map to tuple2 structure
                .flatMap(new FlatMapFunction<String, LinkPair>() {
                    @Override
                    public void flatMap(String s, Collector<LinkPair> out) throws Exception {
                        String[] cols = s.split(Pattern.quote("|"));
                        if (cols.length >= 2) {
                            String[] links = cols[1].split(Pattern.quote("#"));

                            for (String link : links) {
                                out.collect(new LinkPair(
                                        cols[0],
                                        link
                                ));
                            }
                        } else {
                            throw new Exception("Cannot read See also input: " + s);
                        }
                    }
                }), redirects);
    }


    public static DataSet<Tuple3<String, String, Integer>> resolveRedirects(DataSet<LinkPair> seeAlso, DataSet<RedirectMapping> redirects) {
        // TODO Rewrite as leftOuterJoin function
        return SeeAlsoExtractor.getSeeAlsoOutput(seeAlso
                .coGroup(redirects)
                .where(1) // see also link
                .equalTo(0) // redirect source
                // replace
                .with(new CoGroupFunction<LinkPair, RedirectMapping, LinkPair>() {
                    @Override
                    public void coGroup(Iterable<LinkPair> seeAlso, Iterable<RedirectMapping> redirect, Collector<LinkPair> out) throws Exception {
                        Iterator<LinkPair> iteratorSeeAlso = seeAlso.iterator();
                        Iterator<RedirectMapping> iteratorRedirect = redirect.iterator();

                        while (iteratorSeeAlso.hasNext()) {
                            LinkPair recordSeeAlso = iteratorSeeAlso.next();

                            if (iteratorRedirect.hasNext()) {
                                RedirectMapping recordRedirect = iteratorRedirect.next();

                                // replace
                                recordSeeAlso.setField(recordRedirect.getTarget(), 1);
                            }
                            out.collect(recordSeeAlso);
                        }
                    }
                }));
    }

}
