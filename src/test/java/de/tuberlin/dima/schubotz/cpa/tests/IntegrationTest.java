package de.tuberlin.dima.schubotz.cpa.tests;

import de.tuberlin.dima.schubotz.cpa.io.WikiDocumentEmitter;
import de.tuberlin.dima.schubotz.cpa.types.LinkTuple;
import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;
import org.junit.Test;

import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import static org.junit.Assert.assertThat;
import static org.junit.matchers.JUnitMatchers.containsString;

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
	public void testNormalDoc(){
		String docString = getFileContents("wikienmathsample.xml");
		WikiDocumentEmitter wikiDocumentEmitter = new WikiDocumentEmitter();
		final Configuration parameters = new Configuration();
        // TODO: Figure out what's wrong with that
        // see https://github.com/stratosphere/stratosphere/wiki/Release-0.4-Codename-%22Ozone%22-Planning
		//parameters.setString(eu.stratosphere.api.common.io.FileInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
        parameters.setString("pact.input.file.path", "file:///some/file/that/will/not/be/read");
		wikiDocumentEmitter.configure(parameters);
		Record target = new Record();
		wikiDocumentEmitter.readRecord(target, docString.getBytes(), 0, docString.length());
        WikiDocument doc = target.getField(0, WikiDocument.class);
        assertThat(doc.getText(), containsString("Albedo depends on the [[frequency]] of the radiation."));
        List<Map.Entry<String, Integer>> links = doc.getOutLinks();
        Collector<Record> collector = new Collector<Record>() {
            @Override
            public void collect(Record record) {
                System.out.println(record.getField(0, LinkTuple.class).toString());
            }

            @Override
            public void close() {

            }
        };
        doc.collectLinks(collector);
    }

}
