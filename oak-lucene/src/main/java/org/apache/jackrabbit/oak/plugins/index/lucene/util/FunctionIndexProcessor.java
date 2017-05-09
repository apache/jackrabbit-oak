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
                // currently, all operations involving null return null
                return null;
            }
            stack.push(ps);
        }
        return stack.pop();
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
        Type<?> type = null;
        ArrayList<Object> values = new ArrayList<Object>(ps.count());
        for (int i = 0; i < ps.count(); i++) {
            String s = ps.getValue(Type.STRING, i);
            Object x;
            if ("lower".equals(functionName)) {
                x = s.toLowerCase();
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
        if (match("fn:string-length(") || match("length(")) {
            return "length*" + parse() + read(")");
        }
        int end = remaining.indexOf(')');
        if (end >= 0) {
            remaining = remaining.substring(0, end);
        }
        if (remaining.startsWith("[")) {
            return property(remaining.substring(1, remaining.lastIndexOf(']')).replaceAll("]]", "]"));
        }
        // property name
        return property(remaining.replaceAll("@", ""));
    }
    
    String property(String p) {
        return "@" + p;
    }
    
    private String read(String string) {
        match(string);
        return "";
    }
    
    private boolean match(String string) {
        if (remaining.startsWith(string)) {
            remaining = remaining.substring(string.length());
            return true;
        }
        return false;
    }

}
