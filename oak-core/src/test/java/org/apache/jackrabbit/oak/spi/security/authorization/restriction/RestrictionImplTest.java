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
package org.apache.jackrabbit.oak.spi.security.authorization.restriction;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link RestrictionImpl}
 */
public class RestrictionImplTest {

    private final Type type = Type.NAME;
    private final String name = "test:defName";
    private final String value = "value";
    private final boolean isMandatory = true;
    private RestrictionImpl restriction;

    @Before
    public void before() throws Exception {
        PropertyState property = createProperty(name, value, type);
        restriction = new RestrictionImpl(property, isMandatory);
    }

    private static PropertyState createProperty(String name, String value, Type type) {
        return PropertyStates.createProperty(name, value, type);
    }

    @Test
    public void testGetProperty() {
        assertEquals(createProperty(name, value, type), restriction.getProperty());
    }

    @Test
    public void testGetDefinition() {
        assertEquals(new RestrictionDefinitionImpl(name, type, isMandatory), restriction.getDefinition());
    }

    @Test
    public void testRestrictionFromDefinition() {
        Restriction r = new RestrictionImpl(restriction.getProperty(), restriction.getDefinition());
        assertEquals(restriction, r);
    }

    @Test
    public void testGetName() {
        assertEquals(name, restriction.getDefinition().getName());
    }

    @Test
    public void testGetRequiredType() {
        assertEquals(Type.NAME, restriction.getDefinition().getRequiredType());
    }

    @Test
    public void testIsMandatory() {
        assertTrue(restriction.getDefinition().isMandatory());
    }

    @Test
    public void testInvalid() {
        try {
            new RestrictionImpl(null, false);
            fail("Creating RestrictionDefinition with null name should fail.");
        } catch (NullPointerException e) {
            // success
        }
    }

    @Test
    public void testEquals() {
        // same definition
        assertEquals(restriction, new RestrictionImpl(createProperty(name, value, type), isMandatory));
    }

    @Test
    public void testEqualsSameDefinition() {
        // same definition
        assertEquals(restriction, new RestrictionImpl(restriction.getProperty(), restriction.getDefinition()));
    }

    @Test
    public void testEqualsSameObject() {
        // same object
        assertEquals(restriction, restriction);
    }

    @Test
    public void testNotEqual() {
        List<Restriction> rs = new ArrayList();
        // - different type
        rs.add(new RestrictionImpl(PropertyStates.createProperty(name, value, Type.STRING), isMandatory));
        // - different multi-value status
        rs.add(new RestrictionImpl(PropertyStates.createProperty(name, ImmutableList.of(value), Type.NAMES), isMandatory));
        // - different name
        rs.add(new RestrictionImpl(createProperty("otherName", value, type), isMandatory));
        // - different value
        rs.add(new RestrictionImpl(createProperty(name, "otherValue", type), isMandatory));
        // - different mandatory flag
        rs.add(new RestrictionImpl(createProperty(name, value, type), !isMandatory));
        // - different impl
        rs.add(new Restriction() {
            @Nonnull
            @Override
            public RestrictionDefinition getDefinition() {
                return new RestrictionDefinitionImpl(name, type, isMandatory);
            }

            @Nonnull
            @Override
            public PropertyState getProperty() {
                return createProperty(name, value, type);
            }
        });

        for (Restriction r : rs) {
            assertFalse(restriction.equals(r));
        }
    }

    @Test
    public void testSameHashCode() {
        // same definition
        assertEquals(restriction.hashCode(), new RestrictionImpl(createProperty(name, value, type), isMandatory).hashCode());
    }

    @Test
    public void testNotSameHashCode() {
        List<Restriction> rs = new ArrayList<Restriction>();
        // - different type
        rs.add(new RestrictionImpl(PropertyStates.createProperty(name, value, Type.STRING), isMandatory));
        // - different multi-value status
        rs.add(new RestrictionImpl(PropertyStates.createProperty(name, ImmutableList.of(value), Type.NAMES), isMandatory));
        // - different name
        rs.add(new RestrictionImpl(createProperty("otherName", value, type), isMandatory));
        // - different value
        rs.add(new RestrictionImpl(createProperty(name, "otherValue", type), isMandatory));
        // - different mandatory flag
        rs.add(new RestrictionImpl(createProperty(name, value, type), !isMandatory));
        // - different impl
        rs.add(new Restriction() {
            @Nonnull
            @Override
            public RestrictionDefinition getDefinition() {
                return new RestrictionDefinitionImpl(name, type, isMandatory);
            }

            @Nonnull
            @Override
            public PropertyState getProperty() {
                return createProperty(name, value, type);
            }
        });

        for (Restriction r : rs) {
            assertNotEquals(restriction.hashCode(), r.hashCode());
        }
    }
}
