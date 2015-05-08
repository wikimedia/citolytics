package de.tuberlin.dima.schubotz.cpa.redirects;

import de.tuberlin.dima.schubotz.cpa.utils.WikiSimConfiguration;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Pattern;


public class SeeAlsoRedirects {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        if (args.length <= 2) {
            System.err.print("Error: Parameter missing! Required: SEEALSO REDIRECTS OUTPUT, Current: " + Arrays.toString(args));
            System.exit(1);
        }

        String outputFilename = args[2];

        int pageA = 1;
        int pageB = 2;
        int redirectSource = 0;
        int redirectTarget = 1;

        DataSet<Tuple2<String, String>> redirects = WikiSimRedirects.getRedirectsDataSet(env, args[1]);
        DataSet<Tuple3<String, String, Integer>> seeAlso = env.readTextFile(args[0])
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
                .with(new CoGroupFunction<Tuple2<String, String>, Tuple2<String, String>, Tuple2<String, String>>() {
                    @Override
                    public void coGroup(Iterable<Tuple2<String, String>> seeAlso, Iterable<Tuple2<String, String>> redirect, Collector<Tuple2<String, String>> out) throws Exception {
                        Iterator<Tuple2<String, String>> iteratorSeeAlso = seeAlso.iterator();
                        Iterator<Tuple2<String, String>> iteratorRedirect = redirect.iterator();

                        while (iteratorSeeAlso.hasNext()) {
                            Tuple2<String, String> recordSeeAlso = iteratorSeeAlso.next();

                            if (iteratorRedirect.hasNext()) {
                                Tuple2<String, String> recordRedirect = iteratorRedirect.next();

                                // replace
                                recordSeeAlso.setField(recordRedirect.getField(1), 1);
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

        if (outputFilename.equals("print")) {
            seeAlso.print();
        } else {
            seeAlso.writeAsCsv(outputFilename, WikiSimConfiguration.csvRowDelimiter, String.valueOf(WikiSimConfiguration.csvFieldDelimiter), FileSystem.WriteMode.OVERWRITE);
        }

        env.execute("SeeAlsoRedirects");
    }
}
