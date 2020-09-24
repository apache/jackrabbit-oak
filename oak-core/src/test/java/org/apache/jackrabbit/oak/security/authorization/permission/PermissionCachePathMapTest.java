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
package org.apache.jackrabbit.oak.security.authorization.permission;

import com.google.common.collect.Sets;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_MODIFY_ACCESS_CONTROL;
import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.REP_READ_NODES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PermissionCachePathMapTest extends AbstractCacheTest {

    private PermissionCache cache;

    @Before
    public void before() {
        super.before();

        when(store.load("a")).thenReturn(generatedPermissionEntries("/path1",false, 0, REP_READ_NODES));
        when(store.load("b")).thenReturn(generatedPermissionEntries("/path2", true, 0, JCR_MODIFY_ACCESS_CONTROL));
        when(store.getNumEntries(anyString(), anyLong())).thenReturn(NumEntries.valueOf(1, true));

        Set<String> principalNames = Sets.newHashSet("a", "b");
        assertFalse(permissionCacheBuilder.init(principalNames, createStrategy(250, 10, true)));

        cache = permissionCacheBuilder.build();
        assertEquals(ENTRYMAP_CLASS_NAME, cache.getClass().getName());

        verify(store, times(2)).load(anyString());
        clearInvocations(store);
    }

    @Test
    public void testGetEntriesByPath() {
        assertEquals(1, cache.getEntries("/path1").size());
        assertEquals(1, cache.getEntries("/path2").size());
        assertTrue(cache.getEntries("/any/other/path").isEmpty());

        verify(store, never()).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }

    @Test
    public void testGetEntriesByTree() {
        Tree t = when(mock(Tree.class).getPath()).thenReturn("/path1", "/path2", "/any/other/path").getMock();
        assertEquals(1, cache.getEntries(t).size());
        assertEquals(1, cache.getEntries(t).size());
        assertTrue(cache.getEntries(t).isEmpty());

        verify(store, never()).load(anyString());
        verify(store, never()).load(anyString(), anyString());
    }
}