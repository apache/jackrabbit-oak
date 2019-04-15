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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.ReadOnly;
import org.apache.jackrabbit.oak.plugins.tree.TreeType;
import org.apache.jackrabbit.oak.plugins.tree.TreeTypeAware;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.SecurityProvider;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.Filter;
import org.apache.jackrabbit.oak.spi.security.authorization.principalbased.FilterProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.mockito.Mockito;

import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

final class MockUtility {

    private MockUtility() {}

    static Tree mockTree(@NotNull String name, @Nullable String ntName, boolean exists, @NotNull String... propertyNames) {
        Tree t = mock(Tree.class);
        when(t.exists()).thenReturn(exists);
        when(t.getName()).thenReturn(name);
        if (ntName != null) {
            when(t.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(createPrimaryTypeProperty(ntName));
        }
        for (String propertyName : propertyNames) {
            when(t.hasProperty(propertyName)).thenReturn(true);
            when(t.getProperty(propertyName)).thenReturn(PropertyStates.createProperty(propertyName, "anyValue"));
        }
        return t;
    }

    static Tree mockTree(@NotNull String name, @NotNull String ntName, @NotNull  String path, @NotNull String... propertyNames) {
        Tree t = mock(Tree.class);
        when(t.exists()).thenReturn(true);
        when(t.getName()).thenReturn(name);
        if (ntName != null) {
            when(t.getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(createPrimaryTypeProperty(ntName));
        }
        when(t.getPath()).thenReturn(path);
        when(t.isRoot()).thenReturn(PathUtils.denotesRoot(path));
        for (String propertyName : propertyNames) {
            when(t.hasProperty(propertyName)).thenReturn(true);
            when(t.getProperty(propertyName)).thenReturn(PropertyStates.createProperty(propertyName, "anyValue"));
        }
        return t;
    }

    static Tree mockTree(@NotNull String path, boolean exists) {
        Tree tree = Mockito.mock(Tree.class);
        when(tree.getPath()).thenReturn(path);
        when(tree.exists()).thenReturn(exists);
        when(tree.isRoot()).thenReturn(PathUtils.denotesRoot(path));
        return tree;
    }

    static Tree mockReadOnlyTree(@NotNull TreeType type) {
        Tree readOnly = mock(Tree.class, withSettings().extraInterfaces(ReadOnly.class, TreeTypeAware.class));
        when(((TreeTypeAware) readOnly).getType()).thenReturn(type);
        return readOnly;
    }

    static NodeState mockNodeState(@NotNull String primaryType) {
        return when(mock(NodeState.class).getProperty(JcrConstants.JCR_PRIMARYTYPE)).thenReturn(createPrimaryTypeProperty(primaryType)).getMock();
    }

    static PropertyState createPrimaryTypeProperty(@NotNull String ntName) {
        return PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, ntName, Type.NAME);
    }

    static PropertyState createMixinTypesProperty(@NotNull String... mixinTypes) {
        return PropertyStates.createProperty(JcrConstants.JCR_MIXINTYPES, ImmutableList.copyOf(mixinTypes), Type.NAMES);
    }

    static FilterProvider mockFilterProvider(boolean canHandle) {
        Filter filter = mock(Filter.class);
        when(filter.canHandle(any(Set.class))).thenReturn(canHandle);
        FilterProvider fp = mock(FilterProvider.class);
        when(fp.getFilter(any(SecurityProvider.class), any(Root.class), any(NamePathMapper.class))).thenReturn(filter);
        return fp;
    }
}