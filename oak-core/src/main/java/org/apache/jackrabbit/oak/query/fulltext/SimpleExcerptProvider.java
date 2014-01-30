/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.query.fulltext;

import static org.apache.jackrabbit.util.Text.encodeIllegalXMLCharacters;

import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.query.Query;
import org.apache.jackrabbit.oak.query.QueryImpl;
import org.apache.jackrabbit.oak.query.ast.AndImpl;
import org.apache.jackrabbit.oak.query.ast.ConstraintImpl;
import org.apache.jackrabbit.oak.query.ast.FullTextSearchImpl;
import org.apache.jackrabbit.oak.query.ast.LiteralImpl;
import org.apache.jackrabbit.oak.query.ast.OrImpl;

import com.google.common.collect.ImmutableSet;

/**
 * This class can extract excerpts from node.
 */
public class SimpleExcerptProvider {

    private static final String REP_EXCERPT_FN = "rep:excerpt(.)";

    private static int maxFragmentSize = 150;

    public static String getExcerpt(String path, String columnName,
            Query query, boolean highlight) {
        if (path == null) {
            return null;
        }
        Tree t = query.getTree(path);
        if (t == null || !t.exists()) {
            return null;
        }
        columnName = extractExcerptProperty(columnName);
        if (columnName != null && columnName.contains("/")) {
            for (String p : PathUtils.elements(PathUtils
                    .getParentPath(columnName))) {
                if (t.hasChild(p)) {
                    t = t.getChild(p);
                } else {
                    return null;
                }
            }
            columnName = PathUtils.getName(columnName);
        }

        StringBuilder text = new StringBuilder();
        String separator = "";
        for (PropertyState p : t.getProperties()) {
            if (p.getType().tag() == Type.STRING.tag()
                    && (columnName == null || columnName.equalsIgnoreCase(p
                            .getName()))) {
                text.append(separator);
                separator = " ";
                for (String v : p.getValue(Type.STRINGS)) {
                    text.append(v);
                }
            }
        }
        Set<String> searchToken = extractFulltext(query);
        if (highlight && searchToken != null) {
            String h = highlight(text, searchToken);
            return h;
        }
        return noHighlight(text);
    }

    private static String extractExcerptProperty(String column) {
        // most frequent case first
        if (REP_EXCERPT_FN.equalsIgnoreCase(column)) {
            return null;
        }
        return column.substring(column.indexOf("(") + 1, column.indexOf(")"));
    }

    private static Set<String> extractFulltext(Query q) {
        // TODO instanceof should not be used
        if (q instanceof QueryImpl) {
            return extractFulltext(((QueryImpl) q).getConstraint());
        }
        return ImmutableSet.of();
    }

    private static Set<String> extractFulltext(ConstraintImpl c) {
        Set<String> tokens = new HashSet<String>();
        // TODO instanceof should not be used,
        // as it will break without us noticing if we extend the AST
        if (c instanceof FullTextSearchImpl) {
            FullTextSearchImpl f = (FullTextSearchImpl) c;
            if (f.getFullTextSearchExpression() instanceof LiteralImpl) {
                LiteralImpl l = (LiteralImpl) f.getFullTextSearchExpression();
                tokens.add(l.getLiteralValue().getValue(Type.STRING));
            }
        }
        if (c instanceof AndImpl) {
            AndImpl a = (AndImpl) c;
            tokens.addAll(extractFulltext(a.getConstraint1()));
            tokens.addAll(extractFulltext(a.getConstraint2()));
        }
        if (c instanceof OrImpl) {
            OrImpl o = (OrImpl) c;
            tokens.addAll(extractFulltext(o.getConstraint1()));
            tokens.addAll(extractFulltext(o.getConstraint2()));
        }
        return tokens;
    }

    private static Set<String> tokenize(Set<String> in) {
        Set<String> tokens = new HashSet<String>();
        for (String s : in) {
            tokens.addAll(tokenize(s));
        }
        return tokens;
    }

    private static Set<String> tokenize(String in) {
        Set<String> out = new HashSet<String>();
        StringBuilder token = new StringBuilder();
        boolean quote = false;
        for (int i = 0; i < in.length();) {
            final int c = in.codePointAt(i);
            int length = Character.charCount(c);
            switch (c) {
            case ' ':
                if (quote) {
                    token.append(' ');
                } else if (token.length() > 0) {
                    out.add(token.toString());
                    token = new StringBuilder();
                }
                break;
            case '"':
            case '\'':
                if (quote) {
                    quote = false;
                    if (token.length() > 0) {
                        out.add(token.toString());
                        token = new StringBuilder();
                    }
                } else {
                    quote = true;
                }
                break;
            default:
                token.append(new String(Character.toChars(c)));
            }
            i += length;
        }
        if (token.length() > 0) {
            out.add(token.toString());
        }
        return out;
    }

    private static String noHighlight(StringBuilder text) {
        if (text.length() > maxFragmentSize) {
            int lastSpace = text.lastIndexOf(" ", maxFragmentSize);
            if (lastSpace != -1) {
                text.setLength(lastSpace);
            } else {
                text.setLength(maxFragmentSize);
            }
            text.append(" ...");
        }
        StringBuilder excerpt = new StringBuilder("<div><span>");
        excerpt.append(encodeIllegalXMLCharacters(text.toString()));
        excerpt.append("</span></div>");
        return excerpt.toString();
    }

    private static String highlight(StringBuilder text, Set<String> searchToken) {
        Set<String> tokens = tokenize(searchToken);
        text = new StringBuilder(encodeIllegalXMLCharacters(text.toString()));
        for (String token : tokens) {
            text = replaceAll(text, token, "<strong>", "</strong>");
        }

        StringBuilder excerpt = new StringBuilder("<div><span>");
        excerpt.append(text.toString());
        excerpt.append("</span></div>");
        return excerpt.toString();
    }

    private static StringBuilder replaceAll(StringBuilder in, String token,
            String start, String end) {
        boolean isLike = false;
        if (token.endsWith("*")) {
            token = token.substring(0, token.length() - 1);
            isLike = true;
        }
        int index = in.indexOf(token);
        while (index != -1) {
            int endIndex = index + token.length();
            if (isLike) {
                int nextSpace = in.indexOf(" ", endIndex);
                if (nextSpace != -1) {
                    endIndex = nextSpace;
                } else {
                    endIndex = in.length();
                }
            }
            String current = in.substring(index, endIndex);
            StringBuilder newToken = new StringBuilder(start);
            newToken.append(current);
            newToken.append(end);
            String newTokenS = newToken.toString();
            in.replace(index, index + current.length(), newTokenS);
            index = in.indexOf(token,
                    in.lastIndexOf(newTokenS) + newTokenS.length());
        }
        return in;
    }
}
