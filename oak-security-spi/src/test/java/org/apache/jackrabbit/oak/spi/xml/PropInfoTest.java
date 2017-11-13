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
package org.apache.jackrabbit.oak.spi.xml;

import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.nodetype.PropertyDefinition;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

public class PropInfoTest {

    private TextValue mockTextValue(@Nonnull String value, int type) throws Exception {
        return mockTextValue(value, type, false);
    }

    private TextValue mockTextValue(@Nonnull String value, int type, boolean throwOnDispose) throws Exception {
        Value jcrValue = Mockito.mock(Value.class);
        when(jcrValue.getType()).thenReturn(type);
        when(jcrValue.getString()).thenReturn(value);

        TextValue tv = Mockito.mock(TextValue.class);
        when(tv.getString()).thenReturn(value);
        when(tv.getValue(type)).thenReturn(jcrValue);
        if (throwOnDispose) {
            doThrow(DisposeException.class).when(tv).dispose();
        }

        return tv;
    }

    private PropertyDefinition mockPropDef(int type, boolean isMultiple) {
        PropertyDefinition def = Mockito.mock(PropertyDefinition.class);
        when(def.getRequiredType()).thenReturn(type);
        when(def.isMultiple()).thenReturn(isMultiple);

        return def;
    }

    @Test(expected = DisposeException.class)
    public void testDisposeThrowing() throws Exception {
        PropInfo propInfo = new PropInfo("string", PropertyType.STRING, mockTextValue("value", PropertyType.STRING, true));
        propInfo.dispose();
    }

    @Test(expected = DisposeException.class)
    public void testDisposeMultipleThrowing() throws Exception {
        PropInfo propInfo = new PropInfo("string", PropertyType.STRING, ImmutableList.of(mockTextValue("value", PropertyType.STRING, true)));
        propInfo.dispose();
    }

    @Test
    public void testDisposeMultiple() throws Exception {
        PropInfo propInfo = new PropInfo("string", PropertyType.STRING, ImmutableList.of(mockTextValue("value", PropertyType.STRING, false)));
        propInfo.dispose();
    }

    @Test
    public void getTargetTypeRequiredTypeBoolean() throws Exception {
        PropInfo propInfo = new PropInfo("undef", PropertyType.UNDEFINED, mockTextValue("value",PropertyType.STRING));

        PropertyDefinition def = mockPropDef(PropertyType.BOOLEAN, false);
        assertEquals(PropertyType.BOOLEAN, propInfo.getTargetType(def));
    }

    @Test
    public void getTargetTypeRequiredTypeUndefined() throws Exception {
        PropInfo prop = new PropInfo("long", PropertyType.LONG, mockTextValue("23",PropertyType.LONG));
        PropInfo undef = new PropInfo("undef", PropertyType.UNDEFINED, mockTextValue("value",PropertyType.UNDEFINED));

        PropertyDefinition def = mockPropDef(PropertyType.UNDEFINED, false);

        assertEquals(PropertyType.LONG, prop.getTargetType(def));
        assertEquals(PropertyType.STRING, undef.getTargetType(def));
    }

    @Test
    public void testGetName() throws Exception {
        PropInfo propInfo = new PropInfo("string", PropertyType.STRING, mockTextValue("value", PropertyType.STRING));
        assertEquals("string", propInfo.getName());
    }

    @Test
    public void testGetType() throws Exception {
        PropInfo propInfo = new PropInfo("path", PropertyType.PATH, mockTextValue("/a/path", PropertyType.PATH));
        assertEquals(PropertyType.PATH, propInfo.getType());
    }

    @Test
    public void testIsUnknownMultiple() throws Exception {
        PropInfo propInfo = new PropInfo("boolean", PropertyType.BOOLEAN, ImmutableList.of(mockTextValue("false", PropertyType.BOOLEAN)), PropInfo.MultipleStatus.UNKNOWN);
        assertTrue(propInfo.isUnknownMultiple());

        propInfo = new PropInfo("boolean", PropertyType.BOOLEAN, ImmutableList.of(mockTextValue("false", PropertyType.BOOLEAN)), PropInfo.MultipleStatus.MULTIPLE);
        assertFalse(propInfo.isUnknownMultiple());
    }

    @Test
    public void testIsUnknownMultipleSingle() throws Exception {
        PropInfo propInfo = new PropInfo("long", PropertyType.LONG, mockTextValue("24", PropertyType.LONG));
        assertTrue(propInfo.isUnknownMultiple());
    }

    @Test
    public void testIsUnknownMultipleSingleList() throws Exception {
        PropInfo propInfo = new PropInfo("long", PropertyType.LONG, ImmutableList.of(mockTextValue("24", PropertyType.LONG)));
        assertTrue(propInfo.isUnknownMultiple());
    }

    @Test
    public void testIsUnknownMultipleSingleList2() throws Exception {
        PropInfo propInfo = new PropInfo("long", PropertyType.LONG, ImmutableList.of(mockTextValue("24", PropertyType.LONG)), PropInfo.MultipleStatus.MULTIPLE);
        assertFalse(propInfo.isUnknownMultiple());
    }

    @Test
    public void testIsUnknownMultipleEmpty() throws Exception {
        PropInfo propInfo = new PropInfo("longs", PropertyType.LONG, ImmutableList.of());
        assertFalse(propInfo.isUnknownMultiple());
    }

    @Test
    public void testIsUnknownMultipleMultiple() throws Exception {
        PropInfo propInfo = new PropInfo("longs", PropertyType.LONG, ImmutableList.of(mockTextValue("24", PropertyType.LONG), mockTextValue("44", PropertyType.LONG)));
        assertFalse(propInfo.isUnknownMultiple());
    }

    @Test
    public void testGetTextValueSingle() throws Exception {
        TextValue tv = mockTextValue("value", PropertyType.STRING);
        PropInfo propInfo = new PropInfo("string", PropertyType.STRING, tv);

        assertEquals(tv, propInfo.getTextValue());
    }

    @Test
    public void testGetTextValueSingleList() throws Exception {
        TextValue tv = mockTextValue("value", PropertyType.STRING);
        PropInfo propInfo = new PropInfo("string", PropertyType.STRING, ImmutableList.of(tv));

        assertEquals(tv, propInfo.getTextValue());
    }

    @Test(expected = RepositoryException.class)
    public void testGetTextValueMultiple() throws Exception {
        List<TextValue> tvs = ImmutableList.of(mockTextValue("24", PropertyType.LONG), mockTextValue("35", PropertyType.LONG));
        PropInfo propInfo = new PropInfo("longs", PropertyType.LONG, tvs);

        propInfo.getTextValue();
    }

    @Test
    public void testGetTextValuesSingle() throws Exception {
        TextValue tv = mockTextValue("value", PropertyType.STRING);
        PropInfo propInfo = new PropInfo("string", PropertyType.STRING, tv);

        assertEquals(ImmutableList.of(tv), propInfo.getTextValues());
    }

    @Test
    public void testGetTextValuesMultiple() throws Exception {
        List<TextValue> tvs = ImmutableList.of(mockTextValue("24", PropertyType.LONG));
        PropInfo propInfo = new PropInfo("longs", PropertyType.LONG, tvs);

        assertEquals(tvs, propInfo.getTextValues());
    }

    @Test
    public void testGetValueSingle() throws Exception {
        TextValue tv = mockTextValue("value", PropertyType.STRING);
        PropInfo propInfo = new PropInfo("string", PropertyType.STRING, tv);

        assertEquals(tv.getValue(PropertyType.STRING), propInfo.getValue(PropertyType.STRING));
    }

    @Test
    public void testGetValueSingleList() throws Exception {
        TextValue tv = mockTextValue("value", PropertyType.STRING);
        PropInfo propInfo = new PropInfo("string", PropertyType.STRING, ImmutableList.of(tv));

        assertEquals(tv.getValue(PropertyType.STRING), propInfo.getValue(PropertyType.STRING));
    }

    @Test(expected = RepositoryException.class)
    public void testGetValueMultiple() throws Exception {
        List<TextValue> tvs = ImmutableList.of(mockTextValue("24", PropertyType.LONG), mockTextValue("35", PropertyType.LONG));
        PropInfo propInfo = new PropInfo("longs", PropertyType.LONG, tvs);

        propInfo.getValue(PropertyType.LONG);
    }

    @Test
    public void testGetValuesSingle() throws Exception {
        TextValue tv = mockTextValue("value", PropertyType.STRING);
        PropInfo propInfo = new PropInfo("string", PropertyType.STRING, tv);

        assertEquals(ImmutableList.of(tv.getValue(PropertyType.STRING)), propInfo.getValues(PropertyType.STRING));
    }

    @Test
    public void testGetValuesEmpty() throws Exception {
        PropInfo propInfo = new PropInfo("longs", PropertyType.LONG, ImmutableList.of());

        assertTrue(propInfo.getValues(PropertyType.LONG).isEmpty());
    }

    @Test
    public void testGetValuesMultiple() throws Exception {
        List<TextValue> tvs = ImmutableList.of(mockTextValue("24", PropertyType.LONG));
        PropInfo propInfo = new PropInfo("longs", PropertyType.LONG, tvs);

        assertEquals(Lists.transform(tvs, new Function<TextValue,Value>() {
            @Nullable
            @Override
            public Value apply(TextValue input) {
                try {
                    return input.getValue(PropertyType.LONG);
                } catch (RepositoryException e) {
                    throw new RuntimeException();
                }
            }
        }), propInfo.getValues(PropertyType.LONG));
    }

    @Test
    public void testAsPropertyStateSingle() throws Exception {
        TextValue tv = mockTextValue("value", PropertyType.STRING);
        PropInfo propInfo = new PropInfo("string", PropertyType.STRING, tv);

        PropertyState ps = propInfo.asPropertyState(mockPropDef(PropertyType.STRING, false));
        assertFalse(ps.isArray());
        assertEquals("value", ps.getValue(Type.STRING));
    }

    @Test
    public void testAsPropertyStateEmptyList() throws Exception {
        PropInfo propInfo = new PropInfo("string", PropertyType.STRING, ImmutableList.of());

        PropertyState ps = propInfo.asPropertyState(mockPropDef(PropertyType.STRING, true));
        assertTrue(ps.isArray());
        assertFalse(ps.getValue(Type.STRINGS).iterator().hasNext());
    }

    @Test
    public void testAsPropertyStateSingleList() throws Exception {
        PropInfo propInfo = new PropInfo("strings", PropertyType.STRING, ImmutableList.of(mockTextValue("a", PropertyType.STRING)), PropInfo.MultipleStatus.MULTIPLE);

        PropertyState ps = propInfo.asPropertyState(mockPropDef(PropertyType.STRING, true));
        assertTrue(ps.isArray());
        assertEquals(1, ps.count());
    }

    @Test
    public void testAsPropertyStateMultiples() throws Exception {
        PropInfo propInfo = new PropInfo("strings", PropertyType.STRING, ImmutableList.of(mockTextValue("a", PropertyType.STRING), mockTextValue("b", PropertyType.STRING)));

        PropertyState ps = propInfo.asPropertyState(mockPropDef(PropertyType.STRING, true));
        assertTrue(ps.isArray());
        assertEquals(2, ps.count());
    }

    @Test
    public void testMultipleStatus() {
        assertEquals(PropInfo.MultipleStatus.UNKNOWN, PropInfo.MultipleStatus.valueOf("UNKNOWN"));
        assertEquals(PropInfo.MultipleStatus.MULTIPLE, PropInfo.MultipleStatus.valueOf("MULTIPLE"));
        assertEquals(new PropInfo.MultipleStatus[]{
                PropInfo.MultipleStatus.UNKNOWN, PropInfo.MultipleStatus.MULTIPLE}, PropInfo.MultipleStatus.values());
    }

    private class DisposeException extends RuntimeException {}
}