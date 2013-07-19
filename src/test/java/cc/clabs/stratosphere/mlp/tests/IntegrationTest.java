package cc.clabs.stratosphere.mlp.tests;

import java.io.InputStream;
import java.util.Scanner;

import org.junit.Ignore;
import org.junit.Test;

import cc.clabs.stratosphere.mlp.io.WikiDocumentEmitter;
import cc.clabs.stratosphere.mlp.types.PactFormula;
import cc.clabs.stratosphere.mlp.types.PactFormulaList;
import cc.clabs.stratosphere.mlp.types.PactIdentifiers;
import cc.clabs.stratosphere.mlp.types.WikiDocument;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.pact.common.io.RecordInputFormat;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactString;
import static org.junit.Assert.*;
import static org.junit.matchers.JUnitMatchers.*;
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
	@Ignore
	public void testNormalDoc(){
		String docString = getFileContents("wikienmathsample.xml");
		WikiDocumentEmitter wikiDocumentEmitter = new WikiDocumentEmitter();
		final Configuration parameters = new Configuration();
		parameters.setString(RecordInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
		wikiDocumentEmitter.configure(parameters);
		PactRecord target = new PactRecord();
		wikiDocumentEmitter.readRecord(target, docString.getBytes(), 0, docString.length());
		WikiDocument doc = (WikiDocument) target.getField( 0, WikiDocument.class );
		assertThat(doc.getText(), containsString("Albedo depends on the [[frequency]] of the radiation."));
		PactFormulaList formulae = doc.getFormulas();
		for (PactFormula pactFormula : formulae) {
			System.out.println(pactFormula.getHash());
		}
		PactIdentifiers identifier = doc.getKnownIdentifiers();
		for (PactString pactString : identifier) {
			System.out.println(pactString.toString());
		}
	}
	@Test
	@Ignore
	public void testAugmentedDoc(){
		String docString = getFileContents("augmentendwikitext.xml");
		WikiDocumentEmitter wikiDocumentEmitter = new WikiDocumentEmitter();
		final Configuration parameters = new Configuration();
		parameters.setString(RecordInputFormat.FILE_PARAMETER_KEY, "file:///some/file/that/will/not/be/read");
		wikiDocumentEmitter.configure(parameters);
		PactRecord target = new PactRecord();
		wikiDocumentEmitter.readRecord(target, docString.getBytes(), 0, docString.length());
		WikiDocument doc = (WikiDocument) target.getField( 0, WikiDocument.class );
		assertThat(doc.getText(), containsString("In [[classical mechanics]], the [[equation of motion]] is [[Newton's second law]], and equivalent formulations are the [[Euler–Lagrange equations]] and [[Hamilton's equations]]."));	
//		PactFormulaList formulae = doc.getFormulas();
//		for (PactFormula pactFormula : formulae) {
//			System.out.println(pactFormula.getHash());
//		}
		PactIdentifiers identifier = doc.getKnownIdentifiers();
		for (PactString pactString : identifier) {
			System.out.println(pactString.toString());
		}
	}
	
}
