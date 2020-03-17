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
package org.apache.jackrabbit.oak.spi.security.privilege;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class ImmutablePrivilegeDefinitionTest {

    private ImmutablePrivilegeDefinition def = new ImmutablePrivilegeDefinition("name", true, ImmutableList.of("aggrName"));

    @Test
    public void testGetName() {
        assertEquals("name", def.getName());
    }

    @Test
    public void testIsAbstract() {
        assertTrue(def.isAbstract());
    }

    @Test
    public void testGetDeclaredAggregatedNames() {
        assertEquals(ImmutableSet.of("aggrName"), def.getDeclaredAggregateNames());
    }

    @Test
    public void testGetDeclaredAggregatedNames2() {
        assertTrue(new ImmutablePrivilegeDefinition("name", false, null).getDeclaredAggregateNames().isEmpty());
    }

    @Test
    public void testHashCode() {
        assertEquals(def.hashCode(), def.hashCode());
        assertEquals(def.hashCode(), new ImmutablePrivilegeDefinition(def.getName(), def.isAbstract(), def.getDeclaredAggregateNames()).hashCode());
    }

    @Test
    public void testEquals() {
        assertEquals(def, def);
        assertEquals(def, new ImmutablePrivilegeDefinition(def.getName(), def.isAbstract(), def.getDeclaredAggregateNames()));
    }

    @Test
    public void testNotEquals() {
        PrivilegeDefinition otherDef = Mockito.mock(PrivilegeDefinition.class);
        when(otherDef.getName()).thenReturn(def.getName());
        when(otherDef.isAbstract()).thenReturn(def.isAbstract());
        when(otherDef.getDeclaredAggregateNames()).thenReturn(def.getDeclaredAggregateNames());

        assertNotEquals(def, otherDef);
        assertNotEquals(def, null);
        assertNotEquals(def, new ImmutablePrivilegeDefinition("othername", true, ImmutableList.of("aggrName")));
        assertNotEquals(def, new ImmutablePrivilegeDefinition("name", false, ImmutableList.of("aggrName")));
        assertNotEquals(def, new ImmutablePrivilegeDefinition("name", true, ImmutableList.of("anotherName")));
        assertNotEquals(def, new ImmutablePrivilegeDefinition("name", true, ImmutableList.of()));
        assertNotEquals(def, new ImmutablePrivilegeDefinition("otherName", false, ImmutableList.of("aggrName","aggrName2")));
    }
}