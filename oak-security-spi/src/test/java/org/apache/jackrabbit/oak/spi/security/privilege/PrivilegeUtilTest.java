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

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.Privilege;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_LIFECYCLE_MANAGEMENT;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.PRIVILEGES_PATH;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_AGGREGATES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
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
    
    @Test
    public void testGetPrivilegeOakNameDefaultMapper() throws Exception {
        assertEquals(JCR_READ, PrivilegeUtil.getOakName(JCR_READ, NamePathMapper.DEFAULT));
        assertEquals(Privilege.JCR_ADD_CHILD_NODES, PrivilegeUtil.getOakName(Privilege.JCR_ADD_CHILD_NODES, NamePathMapper.DEFAULT));
        assertEquals("anystring", PrivilegeUtil.getOakName("anystring", NamePathMapper.DEFAULT));
    }

    @Test
    public void testGetPrivilegeOakName() throws Exception {
        NamePathMapper mapper = mock(NamePathMapper.class);
        when(mapper.getOakNameOrNull(Privilege.JCR_LIFECYCLE_MANAGEMENT)).thenReturn(JCR_LIFECYCLE_MANAGEMENT);
        assertEquals(JCR_LIFECYCLE_MANAGEMENT, PrivilegeUtil.getOakName(Privilege.JCR_LIFECYCLE_MANAGEMENT, mapper));
        verify(mapper).getOakNameOrNull(Privilege.JCR_LIFECYCLE_MANAGEMENT);
        verifyNoMoreInteractions(mapper);
    }
    
    @Test(expected = AccessControlException.class)
    public void testGetPrivilegeOakNameFromNull() throws RepositoryException {
        PrivilegeUtil.getOakName(null, NamePathMapper.DEFAULT);
    }

    @Test(expected = AccessControlException.class)
    public void testGetPrivilegeOakNameResolvesToNull() throws RepositoryException {
        PrivilegeUtil.getOakName(Privilege.JCR_READ, new NamePathMapper.Default() {
            public @Nullable String getOakNameOrNull(@NotNull String jcrName) {
                return null;
            }
        });
    }
    
    @Test
    public void testGetPrivilegeOakNamesFromNull() throws Exception {
        NamePathMapper mapper = mock(NamePathMapper.class);
        assertTrue(PrivilegeUtil.getOakNames(null, mapper).isEmpty());
        verifyNoInteractions(mapper);
    }
    
    @Test
    public void testGetPrivilegeOakNamesFromEmptyArray() throws Exception {
        NamePathMapper mapper = mock(NamePathMapper.class);
        assertTrue(PrivilegeUtil.getOakNames(new String[0], mapper).isEmpty());
        verifyNoInteractions(mapper);
    }
}