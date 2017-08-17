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
                } else if (isFullTextCharacter(c) || " +-:&/.".indexOf(c) >= 0) {
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
    
    /**
     * Whether or not the given character is part of a full-text term that
     * should be indexed. Not indexed are punctuation, control characters such as tab,  
     * 
     * See also <a href=
     * "http://en.wikipedia.org/wiki/Character_property_(Unicode)#General_Category"
     * > Unicode Categories</a>.
     * 
     * @param c the character
     * @return true if the character should be indexed
     */
    public static boolean isFullTextCharacter(char c) {
        switch (Character.getType(c)) {
        // Category "Letter" (Lu, Ll, Lt, Lm, Lo)
        case Character.UPPERCASE_LETTER:
        case Character.LOWERCASE_LETTER:
        case Character.TITLECASE_LETTER:
        case Character.MODIFIER_LETTER:
        case Character.OTHER_LETTER:
            return true;
        // Category "Number" (Nd, Nl, No)
        case Character.DECIMAL_DIGIT_NUMBER:
        case Character.LETTER_NUMBER:
        case Character.OTHER_NUMBER:
            return true;
        // Category "Symbol" (Sm, Sc, Sk, So)
        case Character.MATH_SYMBOL:
        case Character.CURRENCY_SYMBOL:
        case Character.MODIFIER_SYMBOL:
        case Character.OTHER_SYMBOL:
            return true;
        // Category "Control" (Cc, Cf)
        case Character.CONTROL:
        case Character.FORMAT:
            return false;
        // Category "Control" (Cs, Co, Cn)
        case Character.SURROGATE:
        case Character.PRIVATE_USE:
        case Character.UNASSIGNED:
            return true;
        // Category "Mark" (Mn, Mc, Me)
        case Character.NON_SPACING_MARK:
        case Character.COMBINING_SPACING_MARK:
        case Character.ENCLOSING_MARK:
            return false;
        // Category "Punctuation" (Pc, Pd, Ps, Pe, Pi, Pf, Po)
        case Character.CONNECTOR_PUNCTUATION:
        case Character.DASH_PUNCTUATION:
        case Character.START_PUNCTUATION:
        case Character.END_PUNCTUATION:
        case Character.INITIAL_QUOTE_PUNCTUATION:
        case Character.FINAL_QUOTE_PUNCTUATION:
        case Character.OTHER_PUNCTUATION:
            return false;
        // Category "Separator" (Zs, Zl, Zp)
        case Character.SPACE_SEPARATOR:
        case Character.LINE_SEPARATOR:
        case Character.PARAGRAPH_SEPARATOR:
            return false;
        }
        // unknown
        return true;
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
    
    @Override
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