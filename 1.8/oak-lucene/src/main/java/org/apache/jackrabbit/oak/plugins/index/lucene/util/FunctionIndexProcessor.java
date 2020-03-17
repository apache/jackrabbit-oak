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
package org.apache.jackrabbit.oak.plugins.index.lucene.util;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.EmptyPropertyState;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
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

    private FunctionIndexProcessor(String function) {
        this.remaining = function;
    }
    
    /**
     * Get the list of properties used in the given function code.
     * 
     * @param functionCode the tokens, for example ["function", "lower", "@name"]
     * @return the list of properties, for example ["name"]
     */
    public static String[] getProperties(String[] functionCode) {
        ArrayList<String> properties = new ArrayList<String>();
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
        Deque<PropertyState> stack = new ArrayDeque<PropertyState>();
        for (int i = functionCode.length - 1; i > 0; i--) {
            String token = functionCode[i];
            PropertyState ps;
            if (token.startsWith("@")) {
                String propertyName = token.substring(1);
                ps = getProperty(path, state, propertyName);
            } else {
                ps = calculateFunction(token, stack);
            }
            if (ps == null) {
                ps = EMPTY_PROPERTY_STATE;
            }
            stack.push(ps);
        }

        PropertyState ret = stack.pop();
        return ret==EMPTY_PROPERTY_STATE ? null : ret;
    }
    
    /**
     * Split the polish notation into a tokens that can more easily be processed.
     *  
     *  @param functionDescription in polish notation, for example "function*lower*{@literal @}name"
     *  @return tokens, for example ["function", "lower", "{@literal @}name"]
     */
    public static String[] getFunctionCode(String functionDescription) {
        if (functionDescription == null) {
            return null;
        }
        return functionDescription.split("\\*");
    }
    
    private static PropertyState calculateFunction(String functionName, 
            Deque<PropertyState> stack) {
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
        ArrayList<Object> values = new ArrayList<Object>(ps.count());
        for (int i = 0; i < ps.count(); i++) {
            String s = ps.getValue(Type.STRING, i);
            Object x;
            if ("lower".equals(functionName)) {
                x = s.toLowerCase();
                type = Type.STRING;
            } else if ("coalesce".equals(functionName)) {
                x = s;
                type = Type.STRING;
            } else if ("upper".equals(functionName)) {
                x = s.toUpperCase();
                type = Type.STRING;
            } else if ("length".equals(functionName)) {
                x = (long) s.length();
                type = Type.LONG;
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
        if (match("fn:upper-case(") || match("upper(")) {
            return "upper*" + parse() + read(")");
        }
        if (match("fn:lower-case(") || match("lower(")) {
            return "lower*" + parse() + read(")");
        }
        if (match("fn:coalesce(") || match("coalesce(")) {
            return "coalesce*" + parse() + readCommaAndWhitespace() + parse() + read(")");
        }
        if (match("fn:string-length(") || match("length(")) {
            return "length*" + parse() + read(")");
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
            return property(prop.replaceAll("]]", "]"));
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
            return property(prop.replaceAll("@", ""));
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
    
    private boolean match(String string) {
        if (remaining.startsWith(string)) {
            remaining = remaining.substring(string.length());
            return true;
        }
        return false;
    }

}
