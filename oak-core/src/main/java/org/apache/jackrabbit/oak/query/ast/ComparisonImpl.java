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

import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.memory.CoreValues;
import org.apache.jackrabbit.oak.query.Query;
import org.apache.jackrabbit.oak.query.index.FilterImpl;

/**
 * A comparison operation (including "like").
 */
public class ComparisonImpl extends ConstraintImpl {

    private final DynamicOperandImpl operand1;
    private final Operator operator;
    private final StaticOperandImpl operand2;

    public ComparisonImpl(DynamicOperandImpl operand1, Operator operator, StaticOperandImpl operand2) {
        this.operand1 = operand1;
        this.operator = operator;
        this.operand2 = operand2;
    }

    public DynamicOperandImpl getOperand1() {
        return operand1;
    }

    public String getOperator() {
        return operator.toString();
    }

    public StaticOperandImpl getOperand2() {
        return operand2;
    }

    @Override
    public boolean evaluate() {
        // JCR 2.0 spec, 6.7.16 Comparison:
        // "operand1 may evaluate to an array of values"
        PropertyState p1 = operand1.currentProperty();
        if (p1 == null) {
            return false;
        }
        // "operand2 always evaluates to a scalar value"
        CoreValue v2 = operand2.currentValue();
        if (v2 == null) {
            // if the property doesn't exist, the result is always false
            // even for "null <> 'x'" (same as in SQL) 
            return false;
        }
        boolean isArray = p1.isArray();
        int v1Type = Query.getType(p1, v2.getType());
        if (v1Type != v2.getType()) {
            // "the value of operand2 is converted to the
            // property type of the value of operand1"
            v2 = query.convert(v2, v1Type);
            if (v2 == null) {
                return false;
            }
        }
        if (!isArray) {
            return evaluate(CoreValues.getValue(p1), v2);
        }
        // for multi-valued properties: if any of the value matches,
        // then return true
        for (CoreValue v1 : CoreValues.getValues(p1)) {
            if (evaluate(v1, v2)) {
                return true;
            }
        }
        return false;
    }

    private boolean evaluate(CoreValue v1, CoreValue v2) {
        switch (operator) {
        case EQUAL:
            return v1.equals(v2);
        case GREATER_OR_EQUAL:
            return v1.compareTo(v2) >= 0;
        case GREATER_THAN:
            return v1.compareTo(v2) > 0;
        case LESS_OR_EQUAL:
            return v1.compareTo(v2) <= 0;
        case LESS_THAN:
            return v1.compareTo(v2) < 0;
        case NOT_EQUAL:
            return !v1.equals(v2);
        case LIKE:
            return evaluateLike(v1, v2);
        }
        throw new IllegalArgumentException("Unknown operator: " + operator);
    }

    private static boolean evaluateLike(CoreValue v1, CoreValue v2) {
        LikePattern like = new LikePattern(v2.getString());
        return like.matches(v1.getString());
    }

    @Override
    boolean accept(AstVisitor v) {
        return v.visit(this);
    }

    @Override
    public String toString() {
        return operand1 + " " + operator + " " + operand2;
    }

    @Override
    public void restrict(FilterImpl f) {
        CoreValue v = operand2.currentValue();
        if (v != null) {
            operand1.restrict(f, operator, v);
            // TODO OAK-347
//            if (operator == Operator.LIKE) {
//                String pattern;
//                pattern = v.getString();
//                LikePattern p = new LikePattern(pattern);
//                String lowerBound = p.getLowerBound();
//                String upperBound = p.getUpperBound();
//                if (lowerBound == null && upperBound == null) {
//                    // ignore
//                } else if (operand1.supportsRangeConditions()) {
//                    CoreValueFactory vf = query.getValueFactory();
//                    if (lowerBound != null) {
//                        operand1.restrict(f, Operator.GREATER_OR_EQUAL, vf.createValue(lowerBound));
//                    }
//                    if (upperBound != null) {
//                        operand1.restrict(f, Operator.LESS_OR_EQUAL, vf.createValue(upperBound));
//                    }
//                }
//            } else {
//                operand1.restrict(f, operator, v);
//            }
        }
    }

    @Override
    public void restrictPushDown(SelectorImpl s) {
        CoreValue v = operand2.currentValue();
        if (v != null) {
            if (operand1.canRestrictSelector(s)) {
                s.restrictSelector(this);
            }
        }
    }

    /**
     * A pattern matcher.
     */
    public static class LikePattern {

        // TODO LIKE: optimize condition to '=' when no patterns are used, or 'between x and x+1'
        // TODO LIKE: what to do for invalid patterns (patterns ending with a backslash)

        private static final int MATCH = 0, ONE = 1, ANY = 2;

        private String patternString;
        private boolean invalidPattern;
        private char[] patternChars;
        private int[] patternTypes;
        private int patternLength;
        private String lowerBounds, upperBound;

        public LikePattern(String pattern) {
            initPattern(pattern);
            initBounds();
        }

        public boolean matches(String value) {
            return !invalidPattern && compareAt(value, 0, 0, value.length(), patternChars, patternTypes);
        }

        private static boolean compare(char[] pattern, String s, int pi, int si) {
            return pattern[pi] == s.charAt(si);
        }

        private boolean compareAt(String s, int pi, int si, int sLen, char[] pattern, int[] types) {
            for (; pi < patternLength; pi++) {
                int type = types[pi];
                switch (type) {
                case MATCH:
                    if (si >= sLen || !compare(pattern, s, pi, si++)) {
                        return false;
                    }
                    break;
                case ONE:
                    if (si++ >= sLen) {
                        return false;
                    }
                    break;
                case ANY:
                    if (++pi >= patternLength) {
                        return true;
                    }
                    while (si < sLen) {
                        if (compare(pattern, s, pi, si) && compareAt(s, pi, si, sLen, pattern, types)) {
                            return true;
                        }
                        si++;
                    }
                    return false;
                default:
                    throw new IllegalArgumentException("Internal error: " + type);
                }
            }
            return si == sLen;
        }

        private void initPattern(String p) {
            patternLength = 0;
            if (p == null) {
                patternTypes = null;
                patternChars = null;
                return;
            }
            int len = p.length();
            patternChars = new char[len];
            patternTypes = new int[len];
            boolean lastAny = false;
            for (int i = 0; i < len; i++) {
                char c = p.charAt(i);
                int type;
                if (c == '\\') {
                    if (i >= len - 1) {
                        invalidPattern = true;
                        return;
                    }
                    c = p.charAt(++i);
                    type = MATCH;
                    lastAny = false;
                } else if (c == '%') {
                    if (lastAny) {
                        continue;
                    }
                    type = ANY;
                    lastAny = true;
                } else if (c == '_') {
                    type = ONE;
                } else {
                    type = MATCH;
                    lastAny = false;
                }
                patternTypes[patternLength] = type;
                patternChars[patternLength++] = c;
            }
            for (int i = 0; i < patternLength - 1; i++) {
                if (patternTypes[i] == ANY && patternTypes[i + 1] == ONE) {
                    patternTypes[i] = ONE;
                    patternTypes[i + 1] = ANY;
                }
            }
            patternString = new String(patternChars, 0, patternLength);
        }

        @Override
        public String toString() {
            return patternString;
        }

        /**
         * Get the lower bound if any.
         *
         * @return return the lower bound, or null if unbound
         */
        public String getLowerBound() {
            return lowerBounds;
        }

        /**
         * Get the upper bound if any.
         *
         * @return return the upper bound, or null if unbound
         */
        public String getUpperBound() {
            return upperBound;
        }

        private void initBounds() {
            if (invalidPattern) {
                return;
            }
            if (patternLength <= 0 || patternTypes[0] != MATCH) {
                // can't use an index
                return;
            }
            int maxMatch = 0;
            StringBuilder buff = new StringBuilder();
            while (maxMatch < patternLength && patternTypes[maxMatch] == MATCH) {
                buff.append(patternChars[maxMatch++]);
            }
            String lower = buff.toString();
            if (lower.isEmpty()) {
                return;
            }
            if (maxMatch == patternLength) {
                lowerBounds = upperBound = lower;
                return;
            }
            lowerBounds = lower;
            char next = lower.charAt(lower.length() - 1);
            // search the 'next' unicode character (or at least a character
            // that is higher)
            for (int i = 1; i < 2000; i++) {
                String upper = lower.substring(0, lower.length() - 1) + (char) (next + i);
                if (upper.compareTo(lower) > 0) {
                    upperBound = upper;
                    return;
                }
            }
        }

    }

}
