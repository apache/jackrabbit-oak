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
package org.apache.jackrabbit.oak.security.authorization.restriction;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.PropertyType;
import javax.jcr.Value;

import org.apache.jackrabbit.oak.TestNameMapper;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.namepath.NamePathMapperImpl;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.name.Namespaces;
import org.apache.jackrabbit.oak.plugins.value.ValueFactoryImpl;
import org.apache.jackrabbit.oak.spi.security.authorization.AbstractAccessControlTest;
import org.apache.jackrabbit.oak.spi.security.authorization.restriction.Restriction;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link RestrictionImpl}
 */
public class RestrictionImplTest extends AbstractAccessControlTest {

    private String name;
    private RestrictionImpl restriction;

    @Before
    public void before() throws Exception {
        super.before();

        registerNamespace(TestNameMapper.TEST_PREFIX, TestNameMapper.TEST_URI);
        NamePathMapper npMapper = new NamePathMapperImpl(new TestNameMapper(Namespaces.getNamespaceMap(root.getTree("/")), TestNameMapper.LOCAL_MAPPING));

        name = TestNameMapper.TEST_PREFIX + ":defName";
        PropertyState property = createProperty(name);
        restriction = new RestrictionImpl(property, true, npMapper);
    }

    private static PropertyState createProperty(String name) {
        return PropertyStates.createProperty(name, "value", Type.NAME);
    }

    @Test
    public void testGetName() {
        assertEquals(name, restriction.getName());
    }

    @Test
    public void testGetJcrName() {
        assertEquals(TestNameMapper.TEST_LOCAL_PREFIX + ":defName", restriction.getJcrName());
    }

    @Test
    public void testGetRequiredType() {
        assertEquals(PropertyType.NAME, restriction.getRequiredType());
    }

    @Test
    public void testIsMandatory() {
        assertTrue(restriction.isMandatory());
    }

    @Test
    public void testInvalid() {
        try {
            new RestrictionImpl(null, false, namePathMapper);
            fail("Creating RestrictionDefinition with null name should fail.");
        } catch (NullPointerException e) {
            // success
        }

        try {
            new RestrictionImpl(createProperty(name), false, null);
            fail("Creating RestrictionDefinition with null name/path mapper should fail.");
        } catch (NullPointerException e) {
            // success
        }
    }

        @Test
    public void testEquals() {
        // same definition
        assertEquals(restriction, new RestrictionImpl(createProperty(name), true, restriction.getNamePathMapper()));

        // same def but different namepathmapper.
        Restriction r2 = new RestrictionImpl(createProperty(name), true, namePathMapper);
        assertFalse(restriction.getJcrName().equals(r2.getJcrName()));
        assertEquals(restriction, r2);
    }

    @Test
    public void testNotEqual() {
        List<Restriction> rs = new ArrayList<Restriction>();
        // - different type
        rs.add(new RestrictionImpl(PropertyStates.createProperty(name, PropertyType.STRING), true, namePathMapper));
        // - different name
        rs.add(new RestrictionImpl(PropertyStates.createProperty("otherName", PropertyType.NAME), true, namePathMapper));
        // - different mandatory flag
        rs.add(new RestrictionImpl(createProperty(name), false, namePathMapper));
        // - different impl
        rs.add(new Restriction() {
            @Override
            public String getName() {
                return name;
            }
            @Override
            public String getJcrName() {
                throw new UnsupportedOperationException();
            }
            @Override
            public int getRequiredType() {
                return PropertyType.NAME;
            }
            @Override
            public boolean isMandatory() {
                return true;
            }
            @Override
            public PropertyState getProperty() {
                return createProperty(name);
            }

            @Nonnull
            @Override
            public Value getValue() {
                return ValueFactoryImpl.createValue(createProperty(name), namePathMapper);
            }
        });

        for (Restriction r : rs) {
            assertFalse(restriction.equals(r));
        }
    }
}