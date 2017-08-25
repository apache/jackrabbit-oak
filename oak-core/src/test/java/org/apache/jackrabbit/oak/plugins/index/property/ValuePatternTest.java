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
package org.apache.jackrabbit.oak.plugins.index.property;

import static org.apache.jackrabbit.oak.plugins.index.property.ValuePatternUtil.getLongestPrefix;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import com.google.common.collect.Lists;

public class ValuePatternTest {
    
    @Test
    public void getStringsBuilder() {
        NodeBuilder b = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        assertNull(ValuePattern.getStrings(b, "x"));
        
        b.setProperty("x", "");
        assertEquals("[]", ValuePattern.getStrings(b, "x").toString());
        
        b.setProperty("x", "test");
        assertEquals("[test]", ValuePattern.getStrings(b, "x").toString());
        
        PropertyState ps = PropertyStates.createProperty(
                "x",
                Arrays.asList("hello"),
                Type.STRINGS);
        b.setProperty(ps);
        assertEquals("[hello]", ValuePattern.getStrings(b, "x").toString());
        
        ps = PropertyStates.createProperty(
                "x",
                Arrays.asList(),
                Type.STRINGS);
        b.setProperty(ps);
        assertEquals("[]", ValuePattern.getStrings(b, "x").toString());

        ps = PropertyStates.createProperty(
                "x",
                Arrays.asList("a", "b"),
                Type.STRINGS);
        b.setProperty(ps);
        assertEquals("[a, b]", ValuePattern.getStrings(b, "x").toString());
    }
    
    @Test
    public void getStringsState() {
        NodeBuilder b = new MemoryNodeBuilder(EmptyNodeState.EMPTY_NODE);
        assertNull(ValuePattern.getStrings(b.getNodeState(), "x"));
        
        b.setProperty("x", "");
        assertEquals("[]", ValuePattern.getStrings(b.getNodeState(), "x").toString());
        
        b.setProperty("x", "test");
        assertEquals("[test]", ValuePattern.getStrings(b.getNodeState(), "x").toString());
        
        PropertyState ps = PropertyStates.createProperty(
                "x",
                Arrays.asList("hello"),
                Type.STRINGS);
        b.setProperty(ps);
        assertEquals("[hello]", ValuePattern.getStrings(b.getNodeState(), "x").toString());
        
        ps = PropertyStates.createProperty(
                "x",
                Arrays.asList(),
                Type.STRINGS);
        b.setProperty(ps);
        assertEquals("[]", ValuePattern.getStrings(b.getNodeState(), "x").toString());

        ps = PropertyStates.createProperty(
                "x",
                Arrays.asList("a", "b"),
                Type.STRINGS);
        b.setProperty(ps);
        assertEquals("[a, b]", ValuePattern.getStrings(b.getNodeState(), "x").toString());
    }

    @Test
    public void empty() {
        ValuePattern vp = new ValuePattern();
        assertTrue(vp.matches(null));
        assertTrue(vp.matches("x"));
        assertTrue(vp.matchesAll());
        assertTrue(vp.matchesAll(null));
        assertTrue(vp.matchesAll(Collections.emptySet()));
        assertTrue(vp.matchesAll(Collections.singleton("x")));
        assertTrue(vp.matchesAll(new HashSet<String>(Arrays.asList("x", "y"))));
        assertTrue(vp.matchesPrefix(null));
        assertTrue(vp.matchesPrefix(""));
        assertTrue(vp.matchesPrefix("x"));
    }

    @Test
    public void regex() {
        ValuePattern vp = new ValuePattern("x.*", null, null);
        assertTrue(vp.matches(null));
        assertTrue(vp.matches("x"));
        assertFalse(vp.matches("y"));
        assertFalse(vp.matchesAll());
        assertTrue(vp.matchesAll(null));
        assertTrue(vp.matchesAll(Collections.emptySet()));
        assertTrue(vp.matchesAll(Collections.singleton("x")));
        assertFalse(vp.matchesAll(Collections.singleton("y")));
        assertTrue(vp.matchesAll(new HashSet<String>(Arrays.asList("x1", "x2"))));
        assertFalse(vp.matchesAll(new HashSet<String>(Arrays.asList("x1", "y2"))));
        assertFalse(vp.matchesPrefix(null));
        assertFalse(vp.matchesPrefix(""));
        // unkown, as we don't do regular expression analysis
        assertFalse(vp.matchesPrefix("x"));
    }
    
    @Test
    public void included() {
        ValuePattern vp = new ValuePattern(null, Lists.newArrayList("abc", "bcd"), null);
        assertTrue(vp.matches(null));
        assertTrue(vp.matches("abc1"));
        assertTrue(vp.matches("bcd"));
        assertFalse(vp.matches("y"));
        assertFalse(vp.matchesAll());
        assertTrue(vp.matchesAll(null));
        assertTrue(vp.matchesAll(Collections.emptySet()));
        assertTrue(vp.matchesAll(Collections.singleton("abc0")));
        assertFalse(vp.matchesAll(Collections.singleton("c")));
        assertTrue(vp.matchesAll(new HashSet<String>(Arrays.asList("abc1", "bcd1"))));
        assertFalse(vp.matchesAll(new HashSet<String>(Arrays.asList("abc1", "c2"))));
        assertFalse(vp.matchesPrefix(null));
        assertFalse(vp.matchesPrefix(""));
        assertFalse(vp.matchesPrefix("hello"));
        assertTrue(vp.matchesPrefix("abcdef"));
        assertFalse(vp.matchesPrefix("a"));
    }
    
    @Test
    public void excluded() {
        ValuePattern vp = new ValuePattern(null, null, Lists.newArrayList("abc", "bcd"));
        assertTrue(vp.matches(null));
        assertFalse(vp.matches("abc1"));
        assertFalse(vp.matches("bcd"));
        assertTrue(vp.matches("y"));
        assertFalse(vp.matchesAll());
        assertTrue(vp.matchesAll(null));
        assertTrue(vp.matchesAll(Collections.emptySet()));
        assertFalse(vp.matchesAll(Collections.singleton("abc0")));
        assertTrue(vp.matchesAll(Collections.singleton("c")));
        assertFalse(vp.matchesAll(new HashSet<String>(Arrays.asList("abc1", "bcd1"))));
        assertFalse(vp.matchesAll(new HashSet<String>(Arrays.asList("abc1", "c2"))));
        assertTrue(vp.matchesAll(new HashSet<String>(Arrays.asList("c2", "d2"))));
        assertFalse(vp.matchesPrefix(null));
        assertTrue(vp.matchesPrefix("hello"));
        assertFalse(vp.matchesPrefix("abcdef"));
        assertFalse(vp.matchesPrefix("a"));
    }
    
    @Test
    public void longestPrefix() {
        FilterImpl filter;
        
        filter = new FilterImpl(null, null, null);
        filter.restrictProperty("x", Operator.EQUAL, 
                PropertyValues.newString("hello"));
        assertEquals("hello", getLongestPrefix(filter, "x"));

        filter = new FilterImpl(null, null, null);
        filter.restrictProperty("x", Operator.GREATER_OR_EQUAL, 
                PropertyValues.newString("hello welt"));
        filter.restrictProperty("x", Operator.LESS_OR_EQUAL, 
                PropertyValues.newString("hello world"));
        assertEquals("hello w", getLongestPrefix(filter, "x"));

        filter = new FilterImpl(null, null, null);
        filter.restrictProperty("x", Operator.GREATER_THAN, 
                PropertyValues.newString("hello welt"));
        filter.restrictProperty("x", Operator.LESS_THAN, 
                PropertyValues.newString("hello world"));
        assertEquals("hello w", getLongestPrefix(filter, "x"));

        filter = new FilterImpl(null, null, null);
        filter.restrictProperty("x", Operator.GREATER_THAN, 
                PropertyValues.newString("hello welt"));
        filter.restrictProperty("x", Operator.GREATER_THAN, 
                PropertyValues.newString("hello welt!"));
        filter.restrictProperty("x", Operator.LESS_THAN, 
                PropertyValues.newString("hello world"));
        filter.restrictProperty("x", Operator.EQUAL, 
                PropertyValues.newString("hell"));
        assertEquals("hell", getLongestPrefix(filter, "x"));

        filter = new FilterImpl(null, null, null);
        filter.restrictProperty("x", Operator.GREATER_THAN, 
                PropertyValues.newString("abc"));
        filter.restrictProperty("x", Operator.LESS_THAN, 
                PropertyValues.newString("bde"));
        assertNull(getLongestPrefix(filter, "x"));

        filter = new FilterImpl(null, null, null);
        filter.restrictProperty("x", Operator.GREATER_THAN, 
                PropertyValues.newString("abc"));
        filter.restrictProperty("x", Operator.GREATER_THAN, 
                PropertyValues.newString("bcd"));
        filter.restrictProperty("x", Operator.LESS_THAN, 
                PropertyValues.newString("dce"));
        assertNull(getLongestPrefix(filter, "x"));

        filter = new FilterImpl(null, null, null);
        filter.restrictProperty("x", Operator.LIKE, 
                PropertyValues.newString("hello%"));
        assertNull(getLongestPrefix(filter, "x"));

        filter = new FilterImpl(null, null, null);
        filter.restrictProperty("x", Operator.GREATER_OR_EQUAL, 
                PropertyValues.newString("hello"));
        assertNull(getLongestPrefix(filter, "x"));

        filter = new FilterImpl(null, null, null);
        filter.restrictProperty("x", Operator.LESS_THAN, 
                PropertyValues.newString("hello"));
        assertNull(getLongestPrefix(filter, "x"));

        filter = new FilterImpl(null, null, null);
        filter.restrictProperty("x", Operator.NOT_EQUAL, null);
        assertNull(getLongestPrefix(filter, "x"));

        filter = new FilterImpl(null, null, null);
        filter.restrictProperty("x", Operator.GREATER_THAN,
                PropertyValues.newString(Arrays.asList("a0", "a1")));
        filter.restrictProperty("x", Operator.LESS_THAN,
                PropertyValues.newString("a2"));
        assertNull(getLongestPrefix(filter, "x"));

        filter = new FilterImpl(null, null, null);
        filter.restrictProperty("x", Operator.GREATER_THAN,
                PropertyValues.newString("a0"));
        filter.restrictProperty("x", Operator.LESS_THAN,
                PropertyValues.newString(Arrays.asList("a3", "a4")));
        assertNull(getLongestPrefix(filter, "x"));
        
    }
    
    
}
