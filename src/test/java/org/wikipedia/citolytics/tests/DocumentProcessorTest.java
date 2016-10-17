package org.wikipedia.citolytics.tests;

import org.junit.Test;
import org.wikipedia.citolytics.tests.utils.Tester;

/**
 * @author malteschwarzer
 */
public class DocumentProcessorTest extends Tester {

    public int findClosingParen(char[] text, int openPos) {
        int closePos = openPos;
        int counter = 1;
        while (counter > 0) {
            char c = text[++closePos];
            if (c == '{') {
                counter++;
            } else if (c == '}') {
                counter--;
            }
        }
        return closePos;
    }

//    public int findClosingParen(String text, int openPos) {
//        String closeStr = "}}";
//        String openStr = "{{";
//        int closeLen = closeStr.length();
//        int openLen = openStr.length();
//
//        int closePos = openPos;
//        int counter = 1;
//        while (counter > 0) {
//            closePos += closeLen;
//            String c = text.substring(closePos-closeLen, closePos);
//            if (c.equals(openStr)) {
//                counter++;
//            }
//            else if (c.equals(closeStr)) {
//                counter--;
//            }
//        }
//        return closePos + closeLen;
//    }

    @Test
    public void TestInfoBoxRemoval() throws Exception {
        String wikiText = getFileContents("wikiInfoBox.xml");

//        System.out.println(wikiText);

//        Pattern p = Pattern.compile("Infobox(.*?)", Pattern.MULTILINE + Pattern.DOTALL);
//        Pattern pp = Pattern.compile("(.*?)}}", Pattern.MULTILINE + Pattern.DOTALL);
//
//        Matcher m = p.matcher(wikiText);

        int startPos = wikiText.indexOf("{{Infobox");
//        int lastPos = start;
        int open = 0;
        char[] text = wikiText.substring(startPos + 2).toCharArray();

//        System.out.println(text);
        int closePos = findClosingParen(text, 0);

//        System.out.println(String.valueOf(text, 0, ));

        String stripped = wikiText.substring(0, startPos) + wikiText.substring(startPos + closePos);

        System.out.println(stripped);

//        int closePos = text.indexOf("}}");
//
//        if(text.indexOf("{{") >= 0) {
//            open++;
//        }


//        if(m.find()) {
//            System.out.println(m.group());
//
//        }
        System.out.println();

    }
}
