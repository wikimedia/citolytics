package org.wikipedia.citolytics.cpa.operators;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.wikipedia.citolytics.cpa.types.LinkPair;
import org.wikipedia.citolytics.cpa.types.LinkPosition;
import org.wikipedia.citolytics.cpa.types.RecommendationPair;
import org.wikipedia.citolytics.cpa.utils.WikiSimConfiguration;
import org.wikipedia.processing.DocumentProcessor;
import org.wikipedia.processing.types.WikiDocument;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.Math.abs;
import static java.lang.Math.max;

/**
 * Processes Wikipedia documents with DocumentProcessor and extracts link pairs that are used for CPA computations.
 */
public class RecommendationPairExtractor extends RichFlatMapFunction<String, RecommendationPair> {

    private DocumentProcessor dp;

    private double[] alphas = new double[]{1.0};
    private boolean enableWiki2006 = false; // WikiDump of 2006 does not contain namespace tags
    private boolean relativeProximity = false;
    private boolean structureProximity = false;
    private boolean backupRecommendations = false;
    private Configuration config;

    public RecommendationPairExtractor() {
    }

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);

        this.config = config;
        enableWiki2006 = config.getBoolean("wiki2006", true);
        relativeProximity = config.getBoolean("relativeProximity", false);
        structureProximity = config.getBoolean("structureProximity", false);
        backupRecommendations = config.getBoolean("backupRecommendations", false);

        String[] arr = config.getString("alpha", "1.0").split(",");

        if(arr.length == 0) {
            throw new Exception("No alpha value specified.");
        }

        alphas = new double[arr.length];
        for (int i = 0; i < arr.length; i++) {
            try {
                alphas[i] = Double.parseDouble(arr[i]);
            } catch(NumberFormatException e) {
                throw new Exception("Could not read alpha value: " + arr[i]);
            }
        }
    }

    private DocumentProcessor getDocumentProcessor() {
        if(dp == null) {
            dp = new DocumentProcessor();

            if(enableWiki2006) {
                dp.enableWiki2006();
            }
            dp.setInfoBoxRemoval(config.getBoolean("removeInfoBox", true));
        }
        return dp;
    }

    @Override
    public void flatMap(String content, Collector<RecommendationPair> out) throws Exception {
        DocumentProcessor dp = getDocumentProcessor();

        WikiDocument doc = dp.processDoc(content);

        if (doc == null) return;
        if (doc.getNS() != 0) return; // Skip all namespaces other than main

        // TODO merge both collect methods
        if (structureProximity) {
            collectLinkPairsBasedOnStructure(doc, out);
        } else {
            collectLinkPairs(doc, out);
        }

        // Backup recommendations (from out-links)
        if(backupRecommendations) {
            collectOutLinks(doc, out);
        }
    }

    /**
     * Collect recommendations based on outgoing links from article. Order by position of first occurrence.
     * Use as source for backup recommendations.
     *
     * @param doc Fully processed WikiDocument
     * @param out
     */
    private void collectOutLinks(WikiDocument doc, Collector<RecommendationPair> out) {
        Set<String> outLinks = new HashSet<>();

        for(Map.Entry<String, Integer> outLink: doc.getOutLinks()) {
            // Only use first occurrence
            if(outLinks.contains(outLink.getKey()))
                continue;

            // Use inverse link position: The closer to the top, the more relevant the link is.
            // TODO Include multiple occurrences?
            double distance = 1. / ((double) outLink.getValue());
//                double distance = ((double) outLink.getValue()) / 1000.;

            double[] cpi = new double[alphas.length];
            Arrays.fill(cpi, distance);

            RecommendationPair pair = new RecommendationPair(doc.getTitle(), outLink.getKey(), distance, cpi);
            out.collect(pair);
            outLinks.add(outLink.getKey());

            // Only collect minimum number of backup recommendations
            if(outLinks.size() >= WikiSimConfiguration.BACKUP_RECOMMENDATION_COUNT) {
                break;
            }
        }
    }

    /**
     * Collect link pairs with alpha-based proximity measure. Proximity is measured as words between links.
     * Proximity relative to document length can be used by setting the relativeProximity parameter to true.
     *
     * CPI(a,b) = |p_d,a - p_d,b| ^ -alpha
     * CPI_rel(a,b) = (|p_d,a - p_d,b|/ length(d)) ^ -alpha
     *
     * - where p_d,a is the link position of link to a in document d.
     *
     * @param doc Processed and valid Wiki document
     * @param out Collector
     */
    private void collectLinkPairs(WikiDocument doc, Collector<RecommendationPair> out) {

        // Loop all link pairs
        for (Map.Entry<String, Integer> outLink1 : doc.getOutLinks()) {
            for (Map.Entry<String, Integer> outLink2 : doc.getOutLinks()) {
                // Check alphabetical order (A before B)
                String pageA = outLink1.getKey();
                String pageB = outLink2.getKey();
                int order = pageA.compareTo(pageB);

                if (order < 0) {
                    // Retrieve link positions from word map
                    int w1 = doc.getWordMap().floorEntry(outLink1.getValue()).getValue();
                    int w2 = doc.getWordMap().floorEntry(outLink2.getValue()).getValue();

                    // Proximity is defined as number of words between the links
                    double proximity = max(abs(w1 - w2), 1);

                    // Make distance relative to article length (number of words in article)
                    if(relativeProximity) {
                        proximity = proximity / (double) doc.getWordMap().size();
                    }

                    // Collect link pair if is valid
                    if (LinkPair.isValid(pageA, pageB)) {
                        out.collect(new RecommendationPair(pageA, pageB, proximity, computeCPISet(proximity)));
                    }

                }
            }
        }
    }

    /**
     * Compute CPI values for different alpha values
     *
     * @param proximity
     * @return CPI values
     */
    private double[] computeCPISet(double proximity) {
        return computeCPISet(proximity, backupRecommendations);
    }

    private double[] computeCPISet(double proximity, boolean addBackupRecommendationOffset) {
        double[] cpi = new double[alphas.length];

        for (int i=0; i < alphas.length; i++) {
            cpi[i] = computeCPI(proximity, alphas[i]);

            // Add offset to proximity (backup recommendations should be ranked below original recs.)
            if(addBackupRecommendationOffset) {
                cpi[i] += WikiSimConfiguration.BACKUP_RECOMMENDATION_OFFSET;
            }
        }

        return cpi;
    }

    public static double computeCPI(double distance, double alpha) {
        return Math.pow(distance, -alpha);
    }

    /**
     * Collect link pairs with structure-based proximity measure. Proximity is measured with static level.
     *
     * Supported proximity levels:
     * - sentence
     * - paragraph
     * - subsection
     * - section
     * - article
     *
     * @param doc Processed and valid Wiki document
     * @param out Collector
     * @throws Exception Is thrown if alpha values are set
     */
    public void collectLinkPairsBasedOnStructure(WikiDocument doc, Collector<RecommendationPair> out) throws Exception {

        if(alphas.length != 1 && alphas[0] != 1.0) {
            throw new Exception("With using structure-based proximity the alpha values cannot be used");
        }

        String text = doc.getCleanText();

        Pattern hl2_pattern = getHeadlinePattern(2);
        Pattern hl3_pattern = getHeadlinePattern(3);
        Pattern paragraph_pattern = Pattern.compile("^$", Pattern.MULTILINE);

        // Initialize counters
        int hl2_counter = 0, hl3_counter = 0, paragraph_counter = 0;

        Map<String, LinkPosition> links = new HashMap<>();
        String linkTarget;

        // Loop over each structure element and increase counters
        String[] hl2s = hl2_pattern.split(text);
        for (String hl2 : hl2s) {
            String[] hl3s = hl3_pattern.split(hl2);

            for (String hl3 : hl3s) {
                String[] paragraphs = paragraph_pattern.split(hl3);

                for (String paragraph : paragraphs) {
                    // TODO Sentence level
                    // Extract links
                    Matcher m = WikiDocument.LINKS_PATTERN.matcher(paragraph);

                    while (m.find()) {
                        if (m.groupCount() >= 1) {
                            linkTarget = WikiDocument.validateLinkTarget(m.group(1));
                            if (linkTarget != null && !links.containsKey(linkTarget)) {
                                links.put(linkTarget, new LinkPosition(hl2_counter, hl3_counter, paragraph_counter, m.start()));
                            }
                        }
                    }

                    paragraph_counter++;
                }
                hl3_counter++;
            }
            hl2_counter++;
        }

        // Collect link pairs
        for(String pageA: links.keySet()) {
            for(String pageB: links.keySet()) {
                int order = pageA.compareTo(pageB);

                double proximity = links.get(pageA).getProximity(links.get(pageB));

                if (order < 0) {
                    if (LinkPair.isValid(pageA, pageB)) {
                        out.collect(new RecommendationPair(pageA, pageB, proximity, computeCPISet(proximity)));
                    }
                }
            }
        }

    }

    /**
     * Build regex for headline matching depending on headline level, e.g.:
     *
     * = Headline =
     * == Headline 2 ==
     * === Headline 3 ===
     * ...
     *
     * @param level Headline level
     * @return Regex for headlines in Wiki text
     */
    private Pattern getHeadlinePattern(int level) {
        return Pattern.compile("([=]{" + level + "})([\\w\\s]+)([=]{" + level + "})$", Pattern.MULTILINE);
    }

}
