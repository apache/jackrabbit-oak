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
package org.apache.jackrabbit.oak.query.fulltext;

/**
 * A fulltext term, or a "not" term.
 */
public class FullTextTerm extends FullTextExpression {
    private final boolean not;
    private final String propertyName;
    private final String text;
    private final String filteredText;
    private final String boost;
    private final LikePattern like;

    public FullTextTerm(String propertyName, FullTextTerm copy) {
        this.propertyName = propertyName;
        this.not = copy.not;
        this.text = copy.text;
        this.filteredText = copy.filteredText;
        this.boost = copy.boost;
        this.like = copy.like;
    }    

    public FullTextTerm(String propertyName, String text, boolean not, boolean escaped, String boost) {
        this.propertyName = propertyName;
        this.text = text;
        this.not = not;
        this.boost = boost;
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
                } else if (Character.isLetterOrDigit(c) || " +-:&/.".indexOf(c) >= 0) {
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
        // toLowerCase for testFulltextIntercapSQL
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
    public
    FullTextExpression simplify() {
        return this;
    }

    @Override
    public String toString() {
        StringBuilder buff = new StringBuilder();
        if (not) {
            buff.append('-');
        }
        if (propertyName != null && !"*".equals(propertyName)) {
            buff.append(propertyName).append(':');
        }
        buff.append('\"');
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (c == '\\') {
                buff.append(c);
            } else if (c == '\"') {
                buff.append('\\');
            }
            buff.append(c);
        }
        buff.append('\"');
        if (boost != null) {
            buff.append('^').append(boost);
        }
        return buff.toString();
    }
    
    public String getPropertyName() {
        return propertyName;
    }
    
    public String getBoost() {
        return boost;
    }
    
    public boolean isNot() {
        return not;
    }
    
    public String getText() {
        return text;
    }
    
    @Override
    public int getPrecedence() {
        return PRECEDENCE_TERM;
    }
    
    @Override
    public boolean accept(FullTextVisitor v) {
        return v.visit(this);
    }

}