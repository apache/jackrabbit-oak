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
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AbstractAccessControlTest;
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
    private String value = "value";
    private RestrictionImpl restriction;

    @Before
    public void before() throws Exception {
        super.before();

        name = "test:defName";
        PropertyState property = createProperty(name, value);
        restriction = new RestrictionImpl(property, true);
    }

    private static PropertyState createProperty(String name, String value) {
        return PropertyStates.createProperty(name, value, Type.NAME);
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
        assertEquals(restriction, new RestrictionImpl(createProperty(name, value), true));
    }

    @Test
    public void testNotEqual() {
        List<Restriction> rs = new ArrayList<Restriction>();
        // - different type
        rs.add(new RestrictionImpl(PropertyStates.createProperty(name, value, Type.STRING), true));
        // - different multi-value status
        rs.add(new RestrictionImpl(PropertyStates.createProperty(name, ImmutableList.of(value), Type.NAMES), true));
        // - different name
        rs.add(new RestrictionImpl(createProperty("otherName", value), true));
        // - different value
        rs.add(new RestrictionImpl(createProperty("name", "otherValue"), true));
        // - different mandatory flag
        rs.add(new RestrictionImpl(createProperty(name, value), false));
        // - different impl
        rs.add(new Restriction() {
            @Nonnull
            @Override
            public RestrictionDefinition getDefinition() {
                return new RestrictionDefinitionImpl(name, Type.NAME, true);
            }

            @Override
            public PropertyState getProperty() {
                return createProperty(name, value);
            }
        });

        for (Restriction r : rs) {
            assertFalse(restriction.equals(r));
        }
    }
}
