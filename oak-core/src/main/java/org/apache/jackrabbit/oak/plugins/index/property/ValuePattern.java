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
package org.apache.jackrabbit.oak.plugins.index.property;

import java.util.Collections;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * A value pattern.
 */
public class ValuePattern {
    public final static ValuePattern MATCH_ALL = new ValuePattern();

    private final Pattern pattern;
    private final Iterable<String> includePrefixes;
    private final Iterable<String> excludePrefixes;
    
    public ValuePattern(NodeBuilder node) {
        this(node.getString(IndexConstants.VALUE_PATTERN), 
                getStrings(node, IndexConstants.VALUE_INCLUDED_PREFIXES),
                getStrings(node, IndexConstants.VALUE_EXCLUDED_PREFIXES));
    }
    
    public ValuePattern(NodeState node) {
        this(node.getString(IndexConstants.VALUE_PATTERN), 
                getStrings(node, IndexConstants.VALUE_INCLUDED_PREFIXES),
                getStrings(node, IndexConstants.VALUE_EXCLUDED_PREFIXES));
    }
    
    public ValuePattern() {
        this(null, null, null);
    }

    public ValuePattern(String pattern, 
            Iterable<String> includePrefixes, Iterable<String> excludePrefixes) {
        Pattern p = pattern == null ? null : Pattern.compile(pattern);
        this.includePrefixes = includePrefixes;
        this.excludePrefixes = excludePrefixes;
        this.pattern = p;
    }
    
    public boolean matches(String v) {
        if (matchesAll() || v == null) {
            return true;
        }
        if (includePrefixes != null) {
            for(String inc : includePrefixes) {
                if (v.startsWith(inc)) {
                    return true;
                }
            }
        }
        if (excludePrefixes != null) {
            for(String exc : excludePrefixes) {
                if (v.startsWith(exc) || exc.startsWith(v)) {
                    return false;
                }
            }
        }
        if (includePrefixes != null && pattern == null) {
            // we have include prefixes and no pattern
            return false;
        }
        return pattern == null || pattern.matcher(v).matches();
    }
    
    public boolean matchesAll() {
        return includePrefixes == null && excludePrefixes == null && pattern == null;
    }
    
    public static Iterable<String> getStrings(NodeBuilder node, String propertyName) {
        if (!node.hasProperty(propertyName)) {
            return null;
        }
        PropertyState s = node.getProperty(propertyName);
        if (s.isArray()) {
            return node.getProperty(propertyName).getValue(Type.STRINGS);
        }
        return Collections.singleton(node.getString(propertyName));
    }
    
    public static Iterable<String> getStrings(NodeState node, String propertyName) {
        if (!node.hasProperty(propertyName)) {
            return null;
        }
        PropertyState s = node.getProperty(propertyName);
        if (s.isArray()) {
            return node.getStrings(propertyName);
        }
        return Collections.singleton(node.getString(propertyName));
    }
    
    public boolean matchesAll(Set<String> values) {
        if (matchesAll() || values == null) {
            return true;
        }
        for (String v : values) {
            if (!matches(v)) {
                return false;
            }
        }
        return true;
    }

    public boolean matchesPrefix(String prefix) {
        if (pattern != null) {
            // with a regular expression pattern, we don't know
            return false;
        }
        if (includePrefixes == null && excludePrefixes == null) {
            // no includes and excludes
            return true;
        }
        if (prefix == null || prefix.isEmpty()) {
            // we just have "> x" or "< y":
            // comparison is not supported
            return false;
        }
        if (includePrefixes != null) {
            for(String inc : includePrefixes) {
                if (prefix.startsWith(inc)) {
                    return true;
                }
            }
            return false;
        }
        if (excludePrefixes != null) {
            for(String exc : excludePrefixes) {
                if (prefix.startsWith(exc) || exc.startsWith(prefix)) {
                    return false;
                }
            }
        }
        return true;
    }
    
}
