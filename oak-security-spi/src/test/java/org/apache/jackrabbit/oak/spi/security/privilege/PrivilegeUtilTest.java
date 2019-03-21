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
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.Test;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.PRIVILEGES_PATH;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_AGGREGATES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PrivilegeUtilTest {

    @Test
    public void testGetPrivilegesTree() {
        Root root = when(mock(Root.class).getTree(PRIVILEGES_PATH)).thenReturn(mock(Tree.class)).getMock();

        PrivilegeUtil.getPrivilegesTree(root);
        verify(root, times(1)).getTree(PRIVILEGES_PATH);
    }

    @Test
    public void testReadDefinitions() {
        Tree defTree = when(mock(Tree.class).getName()).thenReturn("name").getMock();
        when(defTree.getProperty(PrivilegeConstants.REP_IS_ABSTRACT)).thenReturn(PropertyStates.createProperty(PrivilegeConstants.REP_IS_ABSTRACT, true));

        PrivilegeDefinition def = PrivilegeUtil.readDefinition(defTree);
        assertEquals("name", def.getName());
        assertTrue(def.isAbstract());
        assertTrue(def.getDeclaredAggregateNames().isEmpty());
    }

    @Test
    public void testReadDefinitionsWithAggregates() {
        Iterable<String> aggregateNames = ImmutableSet.of(JCR_READ);
        Tree defTree = when(mock(Tree.class).getProperty(REP_AGGREGATES)).thenReturn(PropertyStates.createProperty(REP_AGGREGATES, aggregateNames, Type.NAMES)).getMock();
        when(defTree.getName()).thenReturn("name");


        PrivilegeDefinition def = PrivilegeUtil.readDefinition(defTree);
        assertEquals("name", def.getName());
        assertTrue(Iterables.elementsEqual(aggregateNames, PrivilegeUtil.readDefinition(defTree).getDeclaredAggregateNames()));

    }
}