/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.spi.query.fulltext;

import java.text.ParseException;
import java.util.ArrayList;


/**
 * A parser for fulltext condition literals. The grammar is defined in the
 * <a href="http://www.day.com/specs/jcr/2.0/6_Query.html#6.7.19">
 * JCR 2.0 specification, 6.7.19 FullTextSearch</a>,
 * as follows (a bit simplified):
 * <pre>
 * FullTextSearchLiteral ::= Disjunct {' OR ' Disjunct}
 * Disjunct ::= Term {' ' Term}
 * Term ::= ['-'] SimpleTerm
 * SimpleTerm ::= Word | '"' Word {' ' Word} '"'
 * </pre>
 */
public class FullTextParser {

    /**
     * Compatibility for Jackrabbit 2.0 single quoted phrase queries.
     * (contains(., "word ''hello world'' word")
     * These are queries that delimit a phrase with a single quote
     * instead, as in the spec, using double quotes.
     */
    private static final boolean JACKRABBIT_2_SINGLE_QUOTED_PHRASE = true;

    private String propertyName;
    private String text;
    private int parseIndex;

    public static FullTextExpression parse(String propertyName, String text) throws ParseException {
        FullTextParser p = new FullTextParser();
        p.propertyName = propertyName;
        p.text = text;
        FullTextExpression e = p.parseOr();
        return e;
    }

    FullTextExpression parseOr() throws ParseException {
        ArrayList<FullTextExpression> list = new ArrayList<FullTextExpression>();
        list.add(parseAnd());
        while (parseIndex < text.length()) {
            if (text.substring(parseIndex).startsWith("OR ")) {
                parseIndex += 3;
                list.add(parseAnd());
            } else {
                break;
            }
        }
        FullTextOr or = new FullTextOr(list);
        return or.simplify();
    }

    FullTextExpression parseAnd() throws ParseException {
        ArrayList<FullTextExpression> list = new ArrayList<FullTextExpression>();
        list.add(parseTerm());
        while (parseIndex < text.length()) {
            if (text.substring(parseIndex).startsWith("OR ")) {
                break;
            }
            list.add(parseTerm());
        }
        FullTextAnd and = new FullTextAnd(list);
        return and.simplify();
    }

    FullTextExpression parseTerm() throws ParseException {
        if (parseIndex >= text.length()) {
            throw getSyntaxError("term");
        }
        boolean not = false;
        StringBuilder buff = new StringBuilder();
        char c = text.charAt(parseIndex);
        if (c == '-' && parseIndex < text.length() - 1 &&
                text.charAt(parseIndex + 1) != ' ') {
            c = text.charAt(++parseIndex);
            not = true;
        }
        boolean escaped = false;
        String boost = null;
        if (c == '\"') {
            parseIndex++;
            while (true) {
                if (parseIndex >= text.length()) {
                    throw getSyntaxError("double quote");
                }
                c = text.charAt(parseIndex++);
                if (c == '\\') {
                    escaped = true;
                    if (parseIndex >= text.length()) {
                        throw getSyntaxError("escaped char");
                    }
                    c = text.charAt(parseIndex++);
                    buff.append(c);
                } else if (c == '\"') {
                    if (parseIndex < text.length()) {
                        if (text.charAt(parseIndex) == '^') {
                            boost = "";
                        } else if (text.charAt(parseIndex) != ' ') {
                            throw getSyntaxError("space");
                        }
                    }
                    parseIndex++;
                    break;
                } else {
                    buff.append(c);
                }
            }
        } else if (c == '\'' && JACKRABBIT_2_SINGLE_QUOTED_PHRASE) {
            // basically the same as double quote
            parseIndex++;
            while (true) {
                if (parseIndex >= text.length()) {
                    throw getSyntaxError("single quote");
                }
                c = text.charAt(parseIndex++);
                if (c == '\\') {
                    escaped = true;
                    if (parseIndex >= text.length()) {
                        throw getSyntaxError("escaped char");
                    }
                    c = text.charAt(parseIndex++);
                    buff.append(c);
                } else if (c == '\'') {
                    if (parseIndex < text.length()) {
                        if (text.charAt(parseIndex) == '^') {
                            boost = "";
                        } else if (text.charAt(parseIndex) != ' ') {
                            throw getSyntaxError("space");
                        }
                    }
                    parseIndex++;
                    break;
                } else {
                    buff.append(c);
                }
            }
        } else {
            do {
                c = text.charAt(parseIndex++);
                if (c == '\\') {
                    escaped = true;
                    if (parseIndex >= text.length()) {
                        throw getSyntaxError("escaped char");
                    }
                    c = text.charAt(parseIndex++);
                    buff.append(c);
                } else if (c == '^') {
                    boost = "";
                    break;
                } else if (c <= ' ') {
                    while (parseIndex < text.length()) {
                        c = text.charAt(parseIndex);
                        if (c > ' ') {
                            break;
                        }
                        parseIndex++;
                    }
                    break;
                } else {
                    buff.append(c);
                }
            } while (parseIndex < text.length());
        }
        if (boost != null) {
            StringBuilder b = new StringBuilder();
            while (parseIndex < text.length()) {
                c = text.charAt(parseIndex++);
                if ((c < '0' || c > '9') && c != '.') {
                    break;
                }
                b.append(c);
            }
            boost = b.toString();
        }
        if (buff.length() == 0) {
            throw getSyntaxError("term");
        }
        String text = buff.toString();
        FullTextTerm term = new FullTextTerm(propertyName, text, not, escaped, boost);
        return term.simplify();
    }

    private ParseException getSyntaxError(String expected) {
        int index = Math.max(0, Math.min(parseIndex, text.length() - 1));
        String query = text.substring(0, index) + "(*)" + text.substring(index).trim();
        if (expected != null) {
            query += "; expected: " + expected;
        }
        return new ParseException("FullText expression: " + query, index);
    }

}
