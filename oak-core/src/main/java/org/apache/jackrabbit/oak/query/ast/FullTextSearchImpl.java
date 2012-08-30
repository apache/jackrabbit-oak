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
package org.apache.jackrabbit.oak.query.ast;

import java.text.ParseException;
import java.util.ArrayList;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.query.ast.ComparisonImpl.LikePattern;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * A fulltext "contains(...)" condition.
 */
public class FullTextSearchImpl extends ConstraintImpl {

    private final String selectorName;
    private final String propertyName;
    private final StaticOperandImpl fullTextSearchExpression;
    private SelectorImpl selector;

    public FullTextSearchImpl(String selectorName, String propertyName,
            StaticOperandImpl fullTextSearchExpression) {
        this.selectorName = selectorName;
        this.propertyName = propertyName;
        this.fullTextSearchExpression = fullTextSearchExpression;
    }

    public StaticOperandImpl getFullTextSearchExpression() {
        return fullTextSearchExpression;
    }

    public String getPropertyName() {
        return propertyName;
    }

    public String getSelectorName() {
        return selectorName;
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("contains(");
        builder.append(getSelectorName());
        if (propertyName != null) {
            builder.append('.');
            builder.append(propertyName);
            builder.append(", ");
        } else {
            builder.append(".*, ");
        }
        builder.append(getFullTextSearchExpression());
        builder.append(')');
        return builder.toString();
    }

    @Override
    public boolean evaluate() {
        StringBuilder buff = new StringBuilder();
        if (propertyName != null) {
            PropertyState p = selector.currentProperty(propertyName);
            if (p == null) {
                return false;
            }
            appendString(buff, p);
        } else {
            Tree tree = getTree(selector.currentPath());
            if (tree == null) {
                return false;
            }
            for (PropertyState p : tree.getProperties()) {
                appendString(buff, p);
            }
        }
        // TODO fulltext conditions: need a way to disable evaluation
        // if a fulltext index is used, to avoid filtering too much
        // (we don't know what exact options are used in the fulltext index)
        // (stop word, special characters,...)
        CoreValue v = fullTextSearchExpression.currentValue();
        try {
            FullTextExpression expr = FullTextParser.parse(v.getString());
            return expr.evaluate(buff.toString());
        } catch (ParseException e) {
            throw new IllegalArgumentException("Invalid expression: " + fullTextSearchExpression, e);
        }
    }

    private static void appendString(StringBuilder buff, PropertyState p) {
        if (p.isArray()) {
            for (CoreValue v : p.getValues()) {
                buff.append(v.getString()).append(' ');
            }
        } else {
            buff.append(p.getValue().getString()).append(' ');
        }
    }

    public void bindSelector(SourceImpl source) {
        selector = source.getExistingSelector(selectorName);
    }

    @Override
    public void apply(FilterImpl f) {
        if (propertyName != null) {
            if (f.getSelector() == selector) {
                f.restrictProperty(propertyName, Operator.NOT_EQUAL, (CoreValue) null);
            }
        }
        f.restrictFulltextCondition(fullTextSearchExpression.currentValue().getString());
    }

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
    public static class FullTextParser {

        String text;
        int parseIndex;

        public static FullTextExpression parse(String text) throws ParseException {
            FullTextParser p = new FullTextParser();
            p.text = text;
            FullTextExpression e = p.parseOr();
            return e;
        }

        FullTextExpression parseOr() throws ParseException {
            FullTextOr or = new FullTextOr();
            or.list.add(parseAnd());
            while (parseIndex < text.length()) {
                if (text.substring(parseIndex).startsWith("OR ")) {
                    parseIndex += 3;
                    or.list.add(parseAnd());
                } else {
                    break;
                }
            }
            return or.simplify();
        }

        FullTextExpression parseAnd() throws ParseException {
            FullTextAnd and = new FullTextAnd();
            and.list.add(parseTerm());
            while (parseIndex < text.length()) {
                if (text.substring(parseIndex).startsWith("OR ")) {
                    break;
                }
                and.list.add(parseTerm());
            }
            return and.simplify();
        }

        FullTextExpression parseTerm() throws ParseException {
            if (parseIndex >= text.length()) {
                throw getSyntaxError("term");
            }
            boolean not = false;
            StringBuilder buff = new StringBuilder();
            char c = text.charAt(parseIndex);
            if (c == '-') {
                if (++parseIndex >= text.length()) {
                    throw getSyntaxError("term");
                }
                not = true;
            }
            boolean escaped = false;
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
                        if (parseIndex < text.length() && text.charAt(parseIndex) != ' ') {
                            throw getSyntaxError("space");
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
                    } else if (c == ' ') {
                        break;
                    } else {
                        buff.append(c);
                    }
                } while (parseIndex < text.length());
            }
            if (buff.length() == 0) {
                throw getSyntaxError("term");
            }
            String text = buff.toString();
            FullTextTerm term = new FullTextTerm(text, not, escaped);
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

    /**
     * The base class for fulltext condition expression.
     */
    public abstract static class FullTextExpression {
        public abstract boolean evaluate(String value);
        abstract FullTextExpression simplify();
    }

    /**
     * A fulltext "and" condition.
     */
    static class FullTextAnd extends FullTextExpression {
        ArrayList<FullTextExpression> list = new ArrayList<FullTextExpression>();

        @Override
        public boolean evaluate(String value) {
            for (FullTextExpression e : list) {
                if (!e.evaluate(value)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        FullTextExpression simplify() {
            return list.size() == 1 ? list.get(0) : this;
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder();
            int i = 0;
            for (FullTextExpression e : list) {
                if (i++ > 0) {
                    buff.append(' ');
                }
                buff.append(e.toString());
            }
            return buff.toString();
        }

    }

    /**
     * A fulltext "or" condition.
     */
    static class FullTextOr extends FullTextExpression {
        ArrayList<FullTextExpression> list = new ArrayList<FullTextExpression>();

        @Override
        public boolean evaluate(String value) {
            for (FullTextExpression e : list) {
                if (e.evaluate(value)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        FullTextExpression simplify() {
            return list.size() == 1 ? list.get(0).simplify() : this;
        }

        @Override
        public String toString() {
            StringBuilder buff = new StringBuilder();
            int i = 0;
            for (FullTextExpression e : list) {
                if (i++ > 0) {
                    buff.append(" OR ");
                }
                buff.append(e.toString());
            }
            return buff.toString();
        }

    }

    /**
     * A fulltext term, or a "not" term.
     */
    static class FullTextTerm extends FullTextExpression {
        private final boolean not;
        private final String text;
        private final String filteredText;
        private final LikePattern like;

        FullTextTerm(String text, boolean not, boolean escaped) {
            this.text = text;
            this.not = not;
            // for testFulltextIntercapSQL
            // filter special characters such as '
            // to make tests pass, for example the
            // FulltextQueryTest.testFulltextExcludeSQL,
            // which searches for:
            // "text ''fox jumps'' -other"
            // (please note the two single quotes instead of
            // double quotes before for and after jumps)
            boolean pattern = false;
            if (escaped) {
                filteredText = text;
            } else {
                StringBuilder buff = new StringBuilder();
                for (int i = 0; i < text.length(); i++) {
                    char c = text.charAt(i);
                    if (c == '*') {
                        buff.append('%');
                        pattern = true;
                    } else if (c == '?') {
                        buff.append('_');
                        pattern = true;
                    } else if (c == '_') {
                        buff.append("\\_");
                        pattern = true;
                    } else if (Character.isLetterOrDigit(c) || " +-:&".indexOf(c) >= 0) {
                        buff.append(c);
                    }
                }
                this.filteredText = buff.toString().toLowerCase();
            }
            if (pattern) {
                like = new LikePattern("%" + filteredText + "%");
            } else {
                like = null;
            }
        }

        @Override
        public boolean evaluate(String value) {
            // for testFulltextIntercapSQL
            value = value.toLowerCase();
            if (like != null) {
                return like.matches(value);
            }
            if (not) {
                return value.indexOf(filteredText) < 0;
            }
            return value.indexOf(filteredText) >= 0;
        }

        @Override
        FullTextExpression simplify() {
            return this;
        }

        @Override
        public String toString() {
            return (not ? "-" : "") + "\"" + text.replaceAll("\"", "\\\"") + "\"";
        }

    }

}
