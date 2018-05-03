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

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.api.Tree;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

public class EmptyPermissionCacheTest {

    private PermissionCache empty;

    @Before
    public void before() {
        PermissionCacheBuilder builder = new PermissionCacheBuilder(Mockito.mock(PermissionStore.class));
        builder.init(ImmutableSet.of(), Long.MAX_VALUE);
        empty = builder.build();
    }

    @Test
    public void testGetEntriesByPath() {
        assertTrue(empty.getEntries("/path").isEmpty());
    }

    @Test
    public void testGetEntriesByTree() {
        Tree tree = Mockito.mock(Tree.class);
        when(tree.getPath()).thenReturn("/path");
        assertTrue(empty.getEntries(tree).isEmpty());
    }
}