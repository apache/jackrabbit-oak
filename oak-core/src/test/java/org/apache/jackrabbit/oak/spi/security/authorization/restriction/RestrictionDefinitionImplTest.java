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

import org.apache.jackrabbit.oak.api.Type;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link RestrictionDefinitionImpl}.
 */
public class RestrictionDefinitionImplTest {

    private String name;
    private RestrictionDefinitionImpl definition;

    @Before
    public void before() throws Exception {
        name = "test:defName";
        definition = new RestrictionDefinitionImpl(name, Type.NAME, true);
    }

    @Test
    public void testGetName() {
        assertEquals(name, definition.getName());
    }

    @Test
    public void testGetRequiredType() {
        assertEquals(Type.NAME, definition.getRequiredType());
    }

    @Test
    public void testIsMandatory() {
        assertTrue(definition.isMandatory());
    }

    @Test
    public void testInvalid() {
        try {
            new RestrictionDefinitionImpl(null, Type.BOOLEAN, false);
            fail("Creating RestrictionDefinition with null name should fail.");
        } catch (NullPointerException e) {
            // success
        }

        try {
            new RestrictionDefinitionImpl(name, Type.UNDEFINED, false);
            fail("Creating RestrictionDefinition with undefined required type should fail.");
        } catch (IllegalArgumentException e) {
            // success
        }
    }

    @Test
    public void testEquals() {
        // same definition
        assertEquals(definition, new RestrictionDefinitionImpl(name, Type.NAME, true));
    }

    @Test
    public void testNotEqual() {
        List<RestrictionDefinition> defs = new ArrayList<RestrictionDefinition>();
        // - different type
        defs.add(new RestrictionDefinitionImpl(name, Type.STRING, true));
        // - different name
        defs.add(new RestrictionDefinitionImpl("otherName", Type.NAME, true));
        // - different mandatory flag
        defs.add(new RestrictionDefinitionImpl(name, Type.NAME, false));
        // - different mv flag
        defs.add(new RestrictionDefinitionImpl(name, Type.NAMES, true));
        // - different impl
        defs.add(new RestrictionDefinition() {
            @Nonnull
            @Override
            public String getName() {
                return name;
            }
            @Nonnull
            @Override
            public Type<?> getRequiredType() {
                return Type.NAME;
            }
            @Override
            public boolean isMandatory() {
                return true;
            }

        });

        for (RestrictionDefinition rd : defs) {
            assertFalse(definition.equals(rd));
        }
    }
}
