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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.tree.TreeConstants;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

/**
 * Test for the hidden {@link TreeConstants#OAK_CHILD_ORDER} property
 */
public class ChildOrderPropertyTest extends AbstractOakCoreTest {

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        Tree a = root.getTree("/a");
        a.setOrderableChildren(true);
        root.commit();
    }

    @Test
    public void testHasProperty() {
        Tree a = root.getTree("/a");
        assertFalse(a.hasProperty(TreeConstants.OAK_CHILD_ORDER));
    }

    @Test
    public void testGetProperty() {
        Tree a = root.getTree("/a");
        assertNull(a.getProperty(TreeConstants.OAK_CHILD_ORDER));
    }

    @Test
    public void testGetProperties() {
        Set<String> propertyNames = Sets.newHashSet(JcrConstants.JCR_PRIMARYTYPE, "aProp");

        Tree a = root.getTree("/a");
        for (PropertyState prop : a.getProperties()) {
            assertTrue(propertyNames.remove(prop.getName()));
        }
        assertTrue(propertyNames.isEmpty());
    }

    @Test
    public void testGetPropertyCount() {
        Tree a = root.getTree("/a");
        assertEquals(2, a.getPropertyCount());
    }

    @Test
    public void testGetPropertyStatus() {
        Tree a = root.getTree("/a");
        assertNull(a.getPropertyStatus(TreeConstants.OAK_CHILD_ORDER));
    }

    @Test
    public void testChildOrderWithoutPropertyReadAccess() throws Exception {
        root.getTree("/a").setOrderableChildren(true);        
        root.getTree("/a/bb").orderBefore("b");
        root.commit();
        setupPermission("/", testPrincipal, true, PrivilegeConstants.REP_READ_NODES);

        Root testRoot = getTestRoot();
        Tree aTree = testRoot.getTree("/a");
        // verify that properties cannot be read:
        assertFalse(aTree.hasProperty(JcrConstants.JCR_PRIMARYTYPE));

        List<String> expected = ImmutableList.of("/a/bb", "/a/b");
        Iterable<String> childPaths = Iterables.transform(aTree.getChildren(), new Function<Tree, String>() {
            @Nullable
            @Override
            public String apply(Tree input) {
                return input.getPath();
            }
        });
        assertTrue(childPaths.toString(), Iterables.elementsEqual(expected, childPaths));
    }
}