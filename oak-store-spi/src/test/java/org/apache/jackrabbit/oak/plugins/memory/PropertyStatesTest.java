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
package org.apache.jackrabbit.oak.plugins.memory;

import static org.junit.Assert.*;

import java.util.Calendar;
import java.util.List;

import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.value.jcr.PartialValueFactory;
import org.apache.jackrabbit.util.ISO8601;
import org.junit.Test;
import org.mockito.Mockito;

public class PropertyStatesTest {

    private final NamePathMapper namePathMapper = Mockito.mock(NamePathMapper.class);
    private final PartialValueFactory valueFactory = new PartialValueFactory(namePathMapper);
    
    @Test
    public void emptyPropertyStateTest() {
        PropertyState s = EmptyPropertyState.emptyProperty("test", Type.STRINGS);
        assertEquals("test", s.getName());
        assertFalse(s.getValue(Type.STRINGS).iterator().hasNext());
        assertTrue(s.isArray());
        assertEquals(Type.STRINGS, s.getType());
        assertEquals(0, s.count());
        try {        
            s.getValue(Type.STRING, 0);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // expected
        }
        try {        
            s.size();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
        try {        
            s.size(0);
            fail();
        } catch (IndexOutOfBoundsException e) {
            // expected
        }
        try {
            s.getValue(Type.STRING);
            fail();
        } catch (IllegalStateException e) {
            // expected
        }
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void emptyPropertyStateNotArrayTest() {
        EmptyPropertyState.emptyProperty("test", Type.STRING);
    }
    
    @Test
    public void multiPropertyStateTest() {
        MultiStringPropertyState s = new MultiStringPropertyState("test", List.of("hello", "world"));
        assertEquals(Type.STRINGS, s.getType());
        assertEquals("test", s.getName());
        assertTrue(s.getValue(Type.STRINGS).iterator().hasNext());
        assertEquals(2, s.count());
        assertEquals("hello".length(), s.size(0));
        assertEquals("world".length(), s.size(1));
        assertEquals("hello", s.getValue(Type.STRING, 0));
        assertEquals("world", s.getValue(Type.STRING, 1));
        try {
            s.getValue(Type.STRING);
            fail();
        } catch (IllegalStateException e) {
            // expected
        } 
        try {        
            s.size();
            fail();
        } catch (IllegalStateException e) {
            // expected
        }        
    }

    @Test
    public void namePropertyFromNameValue() throws RepositoryException {
        PropertyState nameProperty = PropertyStates.createProperty("name", "oak-prefix:value", PropertyType.NAME);
        Value nameValue = valueFactory.createValue(nameProperty);
        PropertyState namePropertyFromValue = PropertyStates.createProperty("name", nameValue);
        assertEquals(nameProperty, namePropertyFromValue);
    }

    @Test
    public void pathPropertyFromPathValue() throws RepositoryException {
        PropertyState pathProperty = PropertyStates.createProperty("path", "oak-prefix:a/oak-prefix:b", PropertyType.PATH);
        Value nameValue = valueFactory.createValue(pathProperty);
        PropertyState namePropertyFromValue = PropertyStates.createProperty("path", nameValue);
        assertEquals(pathProperty, namePropertyFromValue);
    }

    @Test
    public void dateValueFromDateProperty() throws RepositoryException {
        String expected = ISO8601.format(Calendar.getInstance());
        PropertyState dateProperty = PropertyStates.createProperty(
                "date", expected, Type.DATE);
        String actual = dateProperty.getValue(Type.DATE);
        assertEquals(expected, actual);
    }
}
