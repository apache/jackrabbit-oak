/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ShadowInvisibleContentTest extends AbstractOakCoreTest {
     
    @Test
    public void testShadowInvisibleNode() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_ALL);
        setupPermission("/a/b", testPrincipal, false, PrivilegeConstants.JCR_ALL);
        setupPermission("/a/b/c", testPrincipal, true, PrivilegeConstants.JCR_ALL);

        Root testRoot = getTestRoot();
        Tree a = testRoot.getTree("/a");

        // /b not visible to this session
        assertFalse(a.hasChild("b"));

        // shadow /b with transient node of the same name
        Tree b = a.addChild("b");
        assertTrue(a.hasChild("b"));
        assertFalse(b.hasChild("c"));

        try {
            testRoot.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
        }
    }

    @Test
    public void testShadowInvisibleProperty() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_ALL);
        setupPermission("/a", testPrincipal, false, PrivilegeConstants.REP_READ_PROPERTIES);

        Root testRoot = getTestRoot();
        Tree a = testRoot.getTree("/a");

        // /a/x not visible to this session
        assertNull(a.getProperty("aProp"));
        assertFalse(a.hasProperty("aProp"));

        // shadow /a/x with transient property of the same name
        a.setProperty("aProp", "aValue1");
        assertNotNull(a.getProperty("aProp"));
        assertTrue(a.hasProperty("aProp"));

        // after commit() normal access control again takes over!
        testRoot.commit(); // does not fail since only read access is denied
        assertNull(a.getProperty("aProp"));
        assertFalse(a.hasProperty("aProp"));
    }

    @Test
    @Ignore // FIXME how do we handle the case where the shadowing item is the same as the shadowing item?
    public void testShadowInvisibleProperty2() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_ALL);
        setupPermission("/a", testPrincipal, false, PrivilegeConstants.REP_READ_PROPERTIES);

        Root testRoot = getTestRoot();
        Tree a = testRoot.getTree("/a");

        // /a/x not visible to this session
        assertNull(a.getProperty("x"));

        // shadow /a/x with transient property of the same name
        a.setProperty("x", "xValue");
        assertNotNull(a.getProperty("x"));

        try {
            testRoot.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isAccessViolation());
        }
    }

    @Ignore("OAK-869") // FIXME: OAK-869
    @Test
    public void testAddNodeCollidingWithInvisibleNode() throws Exception {
        setupPermission("/a", testPrincipal, true, PrivilegeConstants.JCR_ALL);
        setupPermission("/a/b", testPrincipal, false, PrivilegeConstants.JCR_READ);

        Root testRoot = getTestRoot();
        Tree a = testRoot.getTree("/a");

        assertFalse(a.getChild("b").exists());
        new NodeUtil(a).addChild("b", JcrConstants.NT_UNSTRUCTURED);

        try {
            testRoot.commit();
            fail();
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
        }
    }

}
