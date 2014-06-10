/*        __
 *        \ \
 *   _   _ \ \  ______
 *  | | | | > \(  __  )
 *  | |_| |/ ^ \| || |
 *  | ._,_/_/ \_\_||_|
 *  | |
 *  |_|
 * 
 * ----------------------------------------------------------------------------
 * "THE BEER-WARE LICENSE" (Revision 42):
 * <rob ∂ CLABS dot CC> wrote this file. As long as you retain this notice you
 * can do whatever you want with this stuff. If we meet some day, and you think
 * this stuff is worth it, you can buy me a beer in return.
 * ----------------------------------------------------------------------------
 */
package de.tuberlin.dima.schubotz.cpa.contracts;

import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import de.tuberlin.dima.schubotz.cpa.utils.StringUtils;
import eu.stratosphere.api.java.record.functions.MapFunction;
import eu.stratosphere.types.Record;
import eu.stratosphere.types.StringValue;
import eu.stratosphere.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DocumentProcessor extends MapFunction {

    // private static final Log LOG = LogFactory.getLog(DocumentProcessor.class);


    @Override
    public void map(Record record, Collector<Record> collector) {

        String content = record.getField(0, StringValue.class).getValue();

        // search for a page-xml entity
        Pattern pageRegex = Pattern.compile("(?:<page>\\s+)(?:<title>)(.*?)(?:</title>)\\s+(?:<ns>)(.*?)(?:</ns>)\\s+(?:<id>)(.*?)(?:</id>)(?:.*?)(?:<text.*?>)(.*?)(?:</text>)", Pattern.DOTALL);
        Matcher m = pageRegex.matcher(content);
        // if the record does not contain parsable page-xml
        if (!m.find()) return;

        // otherwise create a WikiDocument object from the xml
        WikiDocument doc = new WikiDocument();
        doc.setId(Integer.parseInt(m.group(3)));
        doc.setTitle(m.group(1));
        doc.setNS(Integer.parseInt(m.group(2)));
        doc.setText(StringUtils.unescapeEntities(m.group(4)));

        // skip docs from namespaces other than
        if (doc.getNS() != 0) return;
        doc.collectLinks(collector);

    }
}








