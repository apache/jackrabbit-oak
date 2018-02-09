/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.doc.doxia.jcr;

import org.h2.bnf.Bnf;
import org.h2.bnf.BnfVisitor;
import org.h2.bnf.Rule;
import org.h2.bnf.RuleFixed;
import org.h2.util.StringUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * A BNF visitor that generates HTML railroad diagrams.
 */
public class BnfRailroad implements BnfVisitor {

    private static final boolean RAILROAD_DOTS = true;

    private static final Map<String, String> XPATH_KEYWORD_TO_ESCAPE = new HashMap();
    static {
        XPATH_KEYWORD_TO_ESCAPE.put("|", "@PIPE@");
        XPATH_KEYWORD_TO_ESCAPE.put("element", "@ELEMENT@");
        XPATH_KEYWORD_TO_ESCAPE.put("/jcr:root", "@JCR_ROOT@");
        XPATH_KEYWORD_TO_ESCAPE.put("jcr:contains", "@CONTAINS@");
        XPATH_KEYWORD_TO_ESCAPE.put("jcr:like", "@LIKE@");
        XPATH_KEYWORD_TO_ESCAPE.put("jcr:score", "@SCORE@");
        XPATH_KEYWORD_TO_ESCAPE.put("fn:not", "@FN_NOT@");
        XPATH_KEYWORD_TO_ESCAPE.put("fn:string-length", "@FN_STRING_LENGTH@");
        XPATH_KEYWORD_TO_ESCAPE.put("fn:name", "@FN_NAME@");
        XPATH_KEYWORD_TO_ESCAPE.put("fn:local-name", "@FN_LOCAL_NAME@");
        XPATH_KEYWORD_TO_ESCAPE.put("fn:lower-case", "@FN_LOWER_CASE@");
        XPATH_KEYWORD_TO_ESCAPE.put("fn:upper-case", "@FN_UPPER_CASE@");
        XPATH_KEYWORD_TO_ESCAPE.put("fn:coalesce", "@FN_COALESCE@");
        XPATH_KEYWORD_TO_ESCAPE.put("rep:excerpt", "@EXCERPT@");
        XPATH_KEYWORD_TO_ESCAPE.put("rep:native", "@NATIVE@");
        XPATH_KEYWORD_TO_ESCAPE.put("rep:similar", "@SIMILAR@");
        XPATH_KEYWORD_TO_ESCAPE.put("rep:spellcheck", "@SPELLCHECK@");
        XPATH_KEYWORD_TO_ESCAPE.put("rep:suggest", "@SUGGEST@");
        XPATH_KEYWORD_TO_ESCAPE.put("rep:facet", "@FACET@");
        XPATH_KEYWORD_TO_ESCAPE.put("text()", "@TEXT@");
        XPATH_KEYWORD_TO_ESCAPE.put("xs:dateTime", "@XS_DATE_TIME@");
        XPATH_KEYWORD_TO_ESCAPE.put("true", "@TRUE@");
        XPATH_KEYWORD_TO_ESCAPE.put("false", "@FALSE@");
        XPATH_KEYWORD_TO_ESCAPE.put("ascending", "@ASCENDING@");
        XPATH_KEYWORD_TO_ESCAPE.put("descending", "@DESCENDING@");
        XPATH_KEYWORD_TO_ESCAPE.put("or", "@OR@");
        XPATH_KEYWORD_TO_ESCAPE.put("and", "@AND@");
        XPATH_KEYWORD_TO_ESCAPE.put("not", "@NOT@");
        XPATH_KEYWORD_TO_ESCAPE.put("explain", "@EXPLAIN@");
        XPATH_KEYWORD_TO_ESCAPE.put("measure", "@MEASURE@");
        XPATH_KEYWORD_TO_ESCAPE.put("order by", "@ORDER_BY@");
        XPATH_KEYWORD_TO_ESCAPE.put("option", "@OPTION@");
        XPATH_KEYWORD_TO_ESCAPE.put("traversal", "@TRAVERSAL@");
        XPATH_KEYWORD_TO_ESCAPE.put("ok", "@OK@");
        XPATH_KEYWORD_TO_ESCAPE.put("warn", "@WARN@");
        XPATH_KEYWORD_TO_ESCAPE.put("fail", "@FAIL@");
        XPATH_KEYWORD_TO_ESCAPE.put("default", "@DEFAULT@");
        XPATH_KEYWORD_TO_ESCAPE.put("index", "@INDEX@");
        XPATH_KEYWORD_TO_ESCAPE.put("tag", "@TAG@");
        XPATH_KEYWORD_TO_ESCAPE.put("fulltextSearchExpression", "@FULLTEXT_EXPRESSION@");
    }

    private BnfSyntax syntaxVisitor;
    private Bnf config;
    private String html;

    /**
     * Generate the HTML for the given syntax.
     *
     * @param bnf the BNF parser
     * @param syntaxLines the syntax
     * @return the HTML
     */
    public String getHtml(Bnf bnf, String syntaxLines) {
        syntaxVisitor = new BnfSyntax();
        this.config = bnf;
        syntaxLines = StringUtils.replaceAll(syntaxLines, "\n    ", " ");
        String[] syntaxList = StringUtils.arraySplit(syntaxLines, '\n', true);
        StringBuilder buff = new StringBuilder();
        for (String s : syntaxList) {
            for (Map.Entry<String, String> entry : XPATH_KEYWORD_TO_ESCAPE.entrySet()) {
                s = StringUtils.replaceAll(s, "'" + entry.getKey() + "'", entry.getValue());
            }

            bnf.visit(this, s);
            html = StringUtils.replaceAll(html, "</code></td>" +
                    "<td class=\"d\"><code class=\"c\">", " ");

            for (Map.Entry<String, String> entry : XPATH_KEYWORD_TO_ESCAPE.entrySet()) {
                html = StringUtils.replaceAll(html, entry.getValue(), entry.getKey());
            }

            if (buff.length() > 0) {
                buff.append("<br />");
            }
            buff.append(html);
        }
        return buff.toString();
    }

    @Override
    public void visitRuleElement(boolean keyword, String name, Rule link) {
        String x;
        if (keyword) {
            x = StringUtils.xmlText(name.trim());
        } else {
            x = syntaxVisitor.getLink(config, name.trim());
        }
        html = "<code class=\"c\">" + x + "</code>";
    }

    @Override
    public void visitRuleRepeat(boolean comma, Rule rule) {
        StringBuilder buff = new StringBuilder();
        if (RAILROAD_DOTS) {
            buff.append("<code class=\"c\">");
            if (comma) {
                buff.append(", ");
            }
            buff.append("...</code>");
        } else {
            buff.append("<table class=\"railroad\">");
            buff.append("<tr class=\"railroad\"><td class=\"te\"></td>");
            buff.append("<td class=\"d\">");
            rule.accept(this);
            buff.append(html);
            buff.append("</td><td class=\"ts\"></td></tr>");
            buff.append("<tr class=\"railroad\"><td class=\"ls\"></td>");
            buff.append("<td class=\"d\">&nbsp;</td>");
            buff.append("<td class=\"le\"></td></tr></table>");
        }
        html = buff.toString();
    }

    @Override
    public void visitRuleFixed(int type) {
        html = getHtmlText(type);
    }

    /**
     * Get the HTML text for the given fixed rule.
     *
     * @param type the fixed rule type
     * @return the HTML text
     */
    static String getHtmlText(int type) {
        switch (type) {
        case RuleFixed.YMD:
            return "2000-01-01";
        case RuleFixed.HMS:
            return "12:00:00";
        case RuleFixed.NANOS:
            return "000000000";
        case RuleFixed.ANY_UNTIL_EOL:
        case RuleFixed.ANY_EXCEPT_SINGLE_QUOTE:
        case RuleFixed.ANY_EXCEPT_DOUBLE_QUOTE:
        case RuleFixed.ANY_WORD:
        case RuleFixed.ANY_EXCEPT_2_DOLLAR:
        case RuleFixed.ANY_UNTIL_END: {
            return "anything";
        }
        case RuleFixed.HEX_START:
            return "0x";
        case RuleFixed.CONCAT:
            return "||";
        case RuleFixed.AZ_UNDERSCORE:
            return "A-Z | _";
        case RuleFixed.AF:
            return "A-F";
        case RuleFixed.DIGIT:
            return "0-9";
        case RuleFixed.OPEN_BRACKET:
            return "[";
        case RuleFixed.CLOSE_BRACKET:
            return "]";
        default:
            throw new AssertionError("type="+type);
        }
    }

    @Override
    public void visitRuleList(boolean or, ArrayList<Rule> list) {
        StringBuilder buff = new StringBuilder();
        if (or) {
            buff.append("<table class=\"railroad\">");
            int i = 0;
            for (Rule r : list) {
                String a = i == 0 ? "t" : i == list.size() - 1 ? "l" : "k";
                i++;
                buff.append("<tr class=\"railroad\"><td class=\"" +
                        a + "s\"></td><td class=\"d\">");
                r.accept(this);
                buff.append(html);
                buff.append("</td><td class=\"" + a + "e\"></td></tr>");
            }
            buff.append("</table>");
        } else {
            buff.append("<table class=\"railroad\">");
            buff.append("<tr class=\"railroad\">");
            for (Rule r : list) {
                buff.append("<td class=\"d\">");
                r.accept(this);
                buff.append(html);
                buff.append("</td>");
            }
            buff.append("</tr></table>");
        }
        html = buff.toString();
    }

    @Override
    public void visitRuleOptional(Rule rule) {
        StringBuilder buff = new StringBuilder();
        buff.append("<table class=\"railroad\">");
        buff.append("<tr class=\"railroad\"><td class=\"ts\"></td>" +
                "<td class=\"d\">&nbsp;</td><td class=\"te\"></td></tr>");
        buff.append("<tr class=\"railroad\">" +
                "<td class=\"ls\"></td><td class=\"d\">");
        rule.accept(this);
        buff.append(html);
        buff.append("</td><td class=\"le\"></td></tr></table>");
        html = buff.toString();
    }

}
