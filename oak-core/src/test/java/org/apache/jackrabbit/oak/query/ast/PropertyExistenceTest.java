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
package org.apache.jackrabbit.oak.query.ast;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.apache.jackrabbit.oak.plugins.memory.PropertyValues;
import org.junit.Test;

public class PropertyExistenceTest {
    
    @Test
    public void coalesce() {
        CoalesceImpl c1 = new CoalesceImpl(
                new PropertyValueImpl("a", "x"), 
                new PropertyValueImpl("b", "y"));
        assertNull(c1.getPropertyExistence());
    }

    @Test
    public void orCoalesce() {
        CoalesceImpl c1 = new CoalesceImpl(
                new PropertyValueImpl("a", "x"), 
                new PropertyValueImpl("b", "y"));
        ComparisonImpl comp1 = new ComparisonImpl(c1, Operator.EQUAL,
                new LiteralImpl(PropertyValues.newString("hello")));        
        CoalesceImpl c2 = new CoalesceImpl(
                new PropertyValueImpl("a", "x"), 
                new PropertyValueImpl("b", "y"));
        ComparisonImpl comp2 = new ComparisonImpl(c2, Operator.EQUAL,
                new LiteralImpl(PropertyValues.newString("world")));
        OrImpl or = new OrImpl(comp1, comp2);
        assertTrue(or.getPropertyExistenceConditions().isEmpty());
    }

    @Test
    public void orX() {
        PropertyValueImpl p1 = new PropertyValueImpl("a", "x");
        ComparisonImpl comp1 = new ComparisonImpl(p1, Operator.EQUAL,
                new LiteralImpl(PropertyValues.newString("hello")));        
        PropertyValueImpl p2 = new PropertyValueImpl("a", "x");
        ComparisonImpl comp2 = new ComparisonImpl(p2, Operator.EQUAL,
                new LiteralImpl(PropertyValues.newString("world")));
        OrImpl or = new OrImpl(comp1, comp2);
        assertEquals("[[a].[x] is not null]", 
                or.getPropertyExistenceConditions().toString());
    }
    
    @Test
    public void in() {
        PropertyValueImpl p1 = new PropertyValueImpl("a", "x");
        InImpl in = new InImpl(p1, Arrays.asList(
                new LiteralImpl(PropertyValues.newString("hello")),
                new LiteralImpl(PropertyValues.newString("hello"))));
        assertEquals("[[a].[x] is not null]", 
                in.getPropertyExistenceConditions().toString());
    }    
    
    @Test
    public void orXY() {
        PropertyValueImpl p1 = new PropertyValueImpl("a", "x");
        ComparisonImpl comp1 = new ComparisonImpl(p1, Operator.EQUAL,
                new LiteralImpl(PropertyValues.newString("hello")));        
        PropertyValueImpl p2 = new PropertyValueImpl("b", "y");
        ComparisonImpl comp2 = new ComparisonImpl(p2, Operator.EQUAL,
                new LiteralImpl(PropertyValues.newString("world")));
        OrImpl or = new OrImpl(comp1, comp2);
        assertEquals("[]", or.getPropertyExistenceConditions().toString());
    }

    @Test
    public void andXY() {
        PropertyValueImpl p1 = new PropertyValueImpl("a", "x");
        ComparisonImpl comp1 = new ComparisonImpl(p1, Operator.EQUAL,
                new LiteralImpl(PropertyValues.newString("hello")));        
        PropertyValueImpl p2 = new PropertyValueImpl("b", "y");
        ComparisonImpl comp2 = new ComparisonImpl(p2, Operator.EQUAL,
                new LiteralImpl(PropertyValues.newString("world")));
        AndImpl or = new AndImpl(comp1, comp2);
        assertEquals("[[a].[x] is not null, [b].[y] is not null]", 
                or.getPropertyExistenceConditions().toString());
    }

}
