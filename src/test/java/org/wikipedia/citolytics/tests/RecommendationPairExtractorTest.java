package org.wikipedia.citolytics.tests;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.junit.Test;
import org.wikipedia.citolytics.cpa.operators.RecommendationPairExtractor;
import org.wikipedia.citolytics.cpa.types.LinkPosition;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.tests.utils.Tester;
import org.wikipedia.processing.DocumentProcessor;
import org.wikipedia.processing.types.WikiDocument;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class RecommendationPairExtractorTest extends Tester {
    @Test
    public void testWordMap() throws Exception {
        String wikiDocStr = getFileContents("RecommendationPairExtractorTest/article.xml.in");

        DocumentProcessor dp = new DocumentProcessor();
        WikiDocument doc = dp.processDoc(wikiDocStr);


        System.out.println(doc.getOutLinks());
        System.out.println(doc.getWordMap());

        for(int i=0; i < 5; i++) {
            Map.Entry<String, Integer> outLink1 = doc.getOutLinks().get(i);
            int w1 = doc.getWordMap().floorEntry(outLink1.getValue()).getValue();

            System.out.println(outLink1);
            System.out.println(w1);
        }

    }

    @Test
    public void testStructureBasedProximity() throws Exception {
        String wikiDocStr = getFileContents("RecommendationPairExtractorTest/article.xml.in");

        DocumentProcessor dp = new DocumentProcessor();
        WikiDocument doc = dp.processDoc(wikiDocStr);

        List<RecommendationPair> list = new ArrayList<>();
        ListCollector<RecommendationPair> collector = new ListCollector<>(list);

        RecommendationPairExtractor rpe = new RecommendationPairExtractor();
        rpe.collectLinkPairsBasedOnStructure(doc, collector);

        System.out.println(list);

        for(RecommendationPair pair: list) {
            assertEquals("Proximity should be paragraph level only", LinkPosition.CPI_PARAGRAPH_LEVEL, pair.getDistance(), 0);

        }
    }
}
