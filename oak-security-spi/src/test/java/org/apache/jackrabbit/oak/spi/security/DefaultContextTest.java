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
package org.apache.jackrabbit.oak.spi.security;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;

import org.apache.jackrabbit.oak.plugins.tree.TreeLocation;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;

public class DefaultContextTest {

    private final Tree tree = Mockito.mock(Tree.class);

    @Test
    public void testDefinesProperty() {
        assertFalse(Context.DEFAULT.definesProperty(tree, Mockito.mock(PropertyState.class)));
    }

    @Test
    public void testDefinesContextRoot() {
        assertFalse(Context.DEFAULT.definesContextRoot(tree));
    }

    @Test
    public void testDefinesTree() {
        assertFalse(Context.DEFAULT.definesTree(tree));
    }

    @Test
    public void testDefinesLocation() {
        assertFalse(Context.DEFAULT.definesLocation(TreeLocation.create(tree)));
    }

    @Test
    public void testDefinesInternal() {
        assertFalse(Context.DEFAULT.definesInternal(tree));
    }
}