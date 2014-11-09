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

import de.tuberlin.dima.schubotz.cpa.types.DataTypes;
import de.tuberlin.dima.schubotz.cpa.types.WikiDocument;
import de.tuberlin.dima.schubotz.cpa.utils.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DocumentProcessor implements FlatMapFunction<String, DataTypes.Result> {
    @Override
    public void flatMap(String content, Collector<DataTypes.Result> out) {

        // search for a page-xml entity
        Matcher m = getPageMatcher(content);
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
        doc.collectLinksAsResult(out);

    }

    public static Matcher getPageMatcher(String content) {
        // TODO skip if <redirect> exists?

        // search for a page-xml entity
        Pattern pageRegex = Pattern.compile("(?:<page>\\s+)(?:<title>)(.*?)(?:</title>)\\s+(?:<ns>)(.*?)(?:</ns>)\\s+(?:<id>)(.*?)(?:</id>)(?:.*?)(?:<text.*?>)(.*?)(?:</text>)", Pattern.DOTALL);
        return pageRegex.matcher(content);
    }
}








