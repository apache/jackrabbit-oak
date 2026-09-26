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
package org.apache.jackrabbit.oak.plugins.index.search.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.spi.query.FunctionIndexUtils;
import org.apache.jackrabbit.oak.spi.query.QueryConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A parser for function-based indexes. It converts the human-readable function
 * definition (XPath) to the internal Polish notation.
 */
public class FunctionIndexProcessor {

    private static final Logger LOG =
            LoggerFactory.getLogger(FunctionIndexProcessor.class);

    private String remaining;

    private static final PropertyState EMPTY_PROPERTY_STATE = EmptyPropertyState.emptyProperty("empty", Type.STRINGS);

    protected FunctionIndexProcessor(String function) {
        this.remaining = function;
    }

    /**
     * Get the list of properties used in the given function code.
     *
     * @param functionCode the tokens, for example ["function", "lower", "@name"]
     * @return the list of properties, for example ["name"]
     */
    public static String[] getProperties(String[] functionCode) {
        ArrayList<String> properties = new ArrayList<>();
        for(String token : functionCode) {
            if (token.startsWith("@")) {
                String propertyName = token.substring(1);
                properties.add(propertyName);
            }
        }
        return properties.toArray(new String[0]);
    }

    /**
     * Try to calculate the value for the given function code.
     *
     * @param path the path of the node
     * @param state the node state
     * @param functionCode the tokens, for example ["function", "lower", "@name"]
     * @return null, or the calculated value
     */
    public static PropertyState tryCalculateValue(String path, NodeState state, String[] functionCode) {
        Deque<PropertyState> stack = new ArrayDeque<>();
        for (int i = functionCode.length - 1; i > 0; i--) {
            String token = functionCode[i];
            PropertyState ps;
            if (token.startsWith("@")) {
                String propertyName = token.substring(1);
                ps = getProperty(path, state, propertyName);
            } else if (isQuotedLiteral(token)) {
                ps = PropertyStates.createProperty("value", unquote(token), Type.STRING);
            } else if ("null".equals(token)) {
                ps = null;
            } else {
                ps = calculateFunction(token, stack);
            }
            if (ps == null) {
                ps = EMPTY_PROPERTY_STATE;
            }
            stack.push(ps);
        }

        PropertyState ret = stack.pop();
        return ret == EMPTY_PROPERTY_STATE ? null : ret;
    }

    private static boolean isQuotedLiteral(String token) {
        return token.length() >= 2 && token.startsWith("'") && token.endsWith("'");
    }

    /**
     * Convert a value popped off the evaluation stack to a {@link PropertyValue},
     * for use with {@link FunctionIndexUtils}. Unlike {@link PropertyValues#create(PropertyState)},
     * this maps the internal {@link #EMPTY_PROPERTY_STATE} "missing value" sentinel to
     * {@code null}, which is the "missing" signal {@link FunctionIndexUtils} expects.
     */
    private static PropertyValue toPropertyValue(PropertyState ps) {
        return ps == EMPTY_PROPERTY_STATE ? null : PropertyValues.create(ps);
    }

    private static String unquote(String token) {
        String inner = token.substring(1, token.length() - 1);
        return inner.replace("''", "'");
    }

    /**
     * Split the polish notation into a tokens that can more easily be processed.
     * This is quote-aware: a token of the form 'text' (as used for the operator
     * literal of op(...)) is kept as one token even if "text" itself contains a
     * '*' character (e.g. the multiplication operator). Within such a literal, a
     * single quote is escaped as two single quotes ('').
     *
     *  @param functionDescription in polish notation, for example "function*lower*{@literal @}name"
     *  @return tokens, for example ["function", "lower", "{@literal @}name"]
     */
    public static String[] getFunctionCode(String functionDescription) {
        if (functionDescription == null) {
            return null;
        }
        ArrayList<String> tokens = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inLiteral = false;
        for (int i = 0; i < functionDescription.length(); i++) {
            char c = functionDescription.charAt(i);
            if (inLiteral) {
                current.append(c);
                if (c == '\'') {
                    if (i + 1 < functionDescription.length() && functionDescription.charAt(i + 1) == '\'') {
                        // an escaped quote within the literal
                        current.append('\'');
                        i++;
                    } else {
                        inLiteral = false;
                    }
                }
            } else if (c == '\'') {
                inLiteral = true;
                current.append(c);
            } else if (c == '*') {
                tokens.add(current.toString());
                current.setLength(0);
            } else {
                current.append(c);
            }
        }
        tokens.add(current.toString());
        return tokens.toArray(new String[0]);
    }

    private static PropertyState calculateFunction(String functionName,
                                                   Deque<PropertyState> stack) {
        if ("if".equals(functionName)) {
            PropertyState condition = stack.pop();
            PropertyState trueValue = stack.pop();
            PropertyState falseValue = stack.pop();
            return FunctionIndexUtils.isTruthy(toPropertyValue(condition)) ?
                    trueValue :
                    falseValue;
        } else if ("exists".equals(functionName)) {
            PropertyState operand = stack.pop();
            return PropertyStates.createProperty("value",
                    operand != EMPTY_PROPERTY_STATE, Type.BOOLEAN);
        } else if ("op".equals(functionName)) {
            PropertyState a = stack.pop();
            PropertyState operator = stack.pop();
            PropertyState b = stack.pop();
            PropertyValue result = FunctionIndexUtils.processOp(
                    toPropertyValue(a),
                    toPropertyValue(operator),
                    toPropertyValue(b));
            if (result == null) {
                return null;
            }
            Type<?> type = result.getType();
            return PropertyStates.createProperty("value", result.getValue(type), type);
        }
        PropertyState ps = stack.pop();
        if ("coalesce".equals(functionName)) {
            // coalesce (a, b) => (a != null ? a : b)
            // we pop stack again to consume the second parameter
            // also, if ps is EMPTY_PROPERTY_STATE, then newly popped value is to be used
            PropertyState ps2 = stack.pop();
            if (ps == EMPTY_PROPERTY_STATE) {
                ps = ps2;
            }
        }
        if (ps == EMPTY_PROPERTY_STATE) {
            return ps;
        }
        Type<?> type = null;
        ArrayList<Object> values = new ArrayList<>(ps.count());
        for (int i = 0; i < ps.count(); i++) {
            Object x;
            if ("lower".equals(functionName)) {
                String s = ps.getValue(Type.STRING, i);
                x = s.toLowerCase();
                type = Type.STRING;
            } else if ("upper".equals(functionName)) {
                String s = ps.getValue(Type.STRING, i);
                x = s.toUpperCase();
                type = Type.STRING;
            } else if ("coalesce".equals(functionName)) {
                x = ps.getValue(Type.STRING, i);
                type = Type.STRING;
            } else if ("length".equals(functionName)) {
                x = ps.size(i);
                type = Type.LONG;
            } else if ("first".equals(functionName)) {
                if (i > 0) {
                    break;
                }
                x = ps.getValue(Type.STRING, 0);
                type = Type.STRING;
            } else {
                LOG.debug("Unknown function {}", functionName);
                return null;
            }
            values.add(x);
        }
        PropertyState result;
        if (values.size() == 1) {
            result = PropertyStates.createProperty("value", values.get(0), type);
        } else {
            type = type.getArrayType();
            result = PropertyStates.createProperty("value", values, type);
        }
        return result;
    }

    private static PropertyState getProperty(String path, NodeState state,
                                             String propertyName) {
        if (PathUtils.getDepth(propertyName) != 1) {
            for(String n : PathUtils.elements(PathUtils.getParentPath(propertyName))) {
                state = state.getChildNode(n);
                if (!state.exists()) {
                    return null;
                }
            }
            propertyName = PathUtils.getName(propertyName);
        }
        PropertyState ps;
        if (":localname".equals(propertyName)) {
            ps = PropertyStates.createProperty("value",
                    getLocalName(PathUtils.getName(path)), Type.STRING);
        } else if (":name".equals(propertyName)) {
            ps = PropertyStates.createProperty("value",
                    PathUtils.getName(path), Type.STRING);
        } else if (":path".equals(propertyName)) {
            ps = PropertyStates.createProperty("value",
                   path, Type.STRING);
        } else {
            ps = state.getProperty(propertyName);
        }
        if (ps == null || ps.count() == 0) {
            return null;
        }
        return ps;
    }

    private static String getLocalName(String name) {
        int colon = name.indexOf(':');
        // TODO LOCALNAME: evaluation of local name might not be correct
        return colon < 0 ? name : name.substring(colon + 1);
    }

    /**
     * Convert a function (in human-readable form) to the polish notation.
     *
     * @param function the function, for example "lower([name])"
     * @return the polish notation, for example "function*lower*{@literal @}name"
     */
    public static String convertToPolishNotation(String function) {
        if (function == null) {
            return null;
        }
        FunctionIndexProcessor p = new FunctionIndexProcessor(function);
        return QueryConstants.FUNCTION_RESTRICTION_PREFIX + p.parse();
    }

    String parse() {
        if (match("fn:local-name()") || match("localname()")) {
            return "@:localname";
        }
        if (match("fn:name()") || match("name()")) {
            return "@:name";
        }
        if (match("fn:path()") || match("path()")) {
            return "@:path";
        }
        if (match("fn:upper-case(") || match("upper(")) {
            return "upper*" + parse() + read(")");
        }
        if (match("fn:lower-case(") || match("lower(")) {
            return "lower*" + parse() + read(")");
        }
        if (match("fn:coalesce(") || match("coalesce(")) {
            return "coalesce*" + parse() + readCommaAndWhitespace() + parse() + read(")");
        }
        if (match("jcr:first(") || match("first(")) {
            return "first*" + parse() + read(")");
        }
        if (match("fn:string-length(") || match("length(")) {
            return "length*" + parse() + read(")");
        }
        if (match("jcr:if(") || match("if(")) {
            return "if*" + parse() + readCommaAndWhitespace() + parse() +
                    readCommaAndWhitespace() + parse() + read(")");
        }
        if (match("jcr:exists(") || match("exists(")) {
            return "exists*" + parse() + read(")");
        }
        if (match("jcr:op(") || match("op(")) {
            return "op*" + parse() + readCommaAndWhitespace() + parse() +
                    readCommaAndWhitespace() + parse() + read(")");
        }
        if (matchNullLiteral() || match("jcr:null()")) {
            return "null";
        }
        if (match("'")) {
            // a quoted string literal, typically used for op()'s operator
            // argument, but usable anywhere a property reference or nested
            // function call is expected. A single quote is escaped as two
            // single quotes ('').
            StringBuilder literal = new StringBuilder();
            while (true) {
                int end = remaining.indexOf('\'');
                if (end < 0) {
                    throw new IllegalArgumentException("Unterminated string literal: " + remaining);
                }
                literal.append(remaining, 0, end);
                remaining = remaining.substring(end + 1);
                if (remaining.startsWith("'")) {
                    // an escaped quote within the literal
                    literal.append("''");
                    remaining = remaining.substring(1);
                } else {
                    break;
                }
            }
            return "'" + literal + "'";
        }

        // property name
        if (match("[")) {
            String prop = remaining;
            int indexOfComma = remaining.indexOf(",");
            if (indexOfComma > 0) {
                prop = remaining.substring(0, indexOfComma);
            }
            prop = prop.substring(0, prop.lastIndexOf(']'));
            remaining = remaining.substring(prop.length() + 1);
            String x = prop.replaceAll("]]", "]");
            if ("jcr:path".equals(x)) {
                return "@:path";
            }
            return property(x);
        } else {
            String prop = remaining;
            int paren = remaining.indexOf(')');
            int comma = remaining.indexOf(',');
            int end = comma;
            if (paren >=0) {
                end = (end < 0) ? paren : Math.min(end, paren);
            }
            if (end >= 0) {
                prop = remaining.substring(0, end);
            }
            remaining = remaining.substring(prop.length());
            String x = prop.replaceAll("@", "");
            if ("jcr:path".equals(x)) {
                return "@:path";
            }
            return property(x);
        }
    }

    String property(String p) {
        return "@" + p;
    }

    private String read(String string) {
        match(string);
        return "";
    }

    private String
    readCommaAndWhitespace() {
        while (match(" ")) {
        }
        match(",");
        while (match(" ")) {
        }
        return "*";
    }

    /**
     * Match the "null" literal, but only as a whole word (so it can never
     * accidentally consume the start of a property or function name).
     */
    private boolean matchNullLiteral() {
        if (remaining.startsWith("null")) {
            String after = remaining.substring(4);
            if (after.isEmpty() || after.startsWith(")") || after.startsWith(",") || after.startsWith(" ")) {
                remaining = after;
                return true;
            }
        }
        return false;
    }

    private boolean match(String string) {
        if (remaining.startsWith(string)) {
            remaining = remaining.substring(string.length());
            return true;
        }
        return false;
    }

}
