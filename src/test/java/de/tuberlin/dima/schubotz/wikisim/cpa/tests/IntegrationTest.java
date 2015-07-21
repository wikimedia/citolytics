package de.tuberlin.dima.schubotz.wikisim.cpa.tests;

import de.tuberlin.dima.schubotz.wikisim.cpa.WikiSim;
import org.junit.Test;

import java.io.InputStream;
import java.util.Scanner;

public class IntegrationTest {
	private String getFileContents(String fname) {
		InputStream is = getClass().getClassLoader().getResourceAsStream(fname);
		Scanner s = new Scanner(is, "UTF-8");
		s.useDelimiter("\\A");
		String out = s.hasNext() ? s.next() : "";
		s.close();
		return out;
	}

    @Test
    public void testNormalDoc() {
        // TODO: flink migration
        /*

        System.out.println("start testnormal Doc");


		String docString = getFileContents("wikienmathsample.xml");
		WikiDocumentEmitter wikiDocumentEmitter = new WikiDocumentEmitter();
		final Configuration parameters = new Configuration();
        // TODO: Figure out what's wrong with that
        // see https://github.com/stratosphere/stratosphere/wiki/Release-0.4-Codename-%22Ozone%22-Planning
		//parameters.setString(eu.stratosphere.api.common.io.FileInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
        parameters.setString("input.file.path", "file:///some/file/that/will/not/be/read");
        wikiDocumentEmitter.configure(parameters);
		Record target = new Record();

        wikiDocumentEmitter.readRecord(target, docString.getBytes(), 0, docString.length());

        //System.out.println(target.getField(0, WikiDocument.class));

        WikiDocument doc;

        try {
            doc = target.getField(0, WikiDocument.class);
        } catch (ClassCastException e) {
            doc = new WikiDocument();
            doc.setText(target.getField(0, StringValue.class).toString());
        }

        assertThat(doc.getText(), containsString("Albedo depends on the [[frequency]] of the radiation."));
        assertEquals("Wrong NS", doc.getNS(), 0);
        /*
        List<Map.Entry<String, Integer>> links = doc.getOutLinks();
        Collector<Record> collector = new Collector<Record>() {
            @Override
            public void collect(Record record) {

                System.out.println(record.getField(0, LinkTupleOld.class).toString());
            }

            @Override
            public void close() {

            }
        };
        doc.collectLinks(collector);
        */
    }

    @Test
    //@Ignore
    public void TestLocalExecution() throws Exception {
        String inputFilename = "file://" + getClass().getClassLoader().getResources("wikienmathsample.xml").nextElement().getPath();
        String outputFilename = "file://" + getClass().getClassLoader().getResources("test.out").nextElement().getPath();

        WikiSim.main(new String[]{inputFilename, outputFilename + Math.random() * Integer.MAX_VALUE, "1.5", "0"});
    }

}
