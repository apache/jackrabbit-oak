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
import org.h2.bnf.RuleHead;
import org.h2.util.StringUtils;

import java.util.ArrayList;
import java.util.StringTokenizer;

/**
 * A BNF visitor that generates BNF in HTML form.
 */
public class BnfSyntax implements BnfVisitor {

    private String html;

    /**
     * Get the HTML syntax for the given syntax.
     *
     * @param bnf the BNF
     * @param syntaxLines the syntax
     * @return the HTML
     */
    public String getHtml(Bnf bnf, String syntaxLines) {
        syntaxLines = StringUtils.replaceAll(syntaxLines, "\n    ", "\n");
        StringTokenizer tokenizer = Bnf.getTokenizer(syntaxLines);
        StringBuilder buff = new StringBuilder();
        while (tokenizer.hasMoreTokens()) {
            String s = tokenizer.nextToken();
            if (s.length() == 1 || StringUtils.toUpperEnglish(s).equals(s)) {
                buff.append(StringUtils.xmlText(s));
                continue;
            }
            buff.append(getLink(bnf, s));
        }
        String s = buff.toString();
        // ensure it works within XHTML comments
        s = StringUtils.replaceAll(s, "--", "&#45;-");
        return s;
    }

    /**
     * Get the HTML link to the given token.
     *
     * @param bnf the BNF
     * @param token the token
     * @return the HTML link
     */
    String getLink(Bnf bnf, String token) {
        RuleHead found = null;
        String key = Bnf.getRuleMapKey(token);
        for (int i = 0; i < token.length(); i++) {
            String test = StringUtils.toLowerEnglish(key.substring(i));
            RuleHead r = bnf.getRuleHead(test);
            if (r != null) {
                found = r;
                break;
            }
        }
        if (found == null) {
            return token;
        }

        if (found.getRule() instanceof RuleFixed) {
            found.getRule().accept(this);
            return html;
        }
        String link = found.getTopic().toLowerCase().replace(' ', '_');
        link = "#" + StringUtils.urlEncode(link);
        return "<a href=\"" + link + "\">" + token + "</a>";
    }

    @Override
    public void visitRuleElement(boolean keyword, String name, Rule link) {
        // not used
    }

    @Override
    public void visitRuleFixed(int type) {
        html = BnfRailroad.getHtmlText(type);
    }

    @Override
    public void visitRuleList(boolean or, ArrayList<Rule> list) {
        // not used
    }

    @Override
    public void visitRuleOptional(Rule rule) {
        // not used
    }

    @Override
    public void visitRuleRepeat(boolean comma, Rule rule) {
        // not used
    }

}
