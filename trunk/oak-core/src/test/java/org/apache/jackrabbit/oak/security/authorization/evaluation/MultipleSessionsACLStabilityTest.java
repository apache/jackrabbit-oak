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

import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests if ACL changes done by different sessions are isolated correctly following the MVCC pattern.
 */
public class MultipleSessionsACLStabilityTest extends AbstractOakCoreTest {

    private Root testRoot1;

    private Root testRoot2;

    private ContentSession testSession1;

    private ContentSession testSession2;

    @Before
    public void before() throws Exception {
        super.before();

        testSession1 = getTestSession();
        String uid = testPrincipal.getName();
        testSession2 = login(new SimpleCredentials(uid, uid.toCharArray()));

        setupPermission("/", testPrincipal, true, PrivilegeConstants.JCR_ALL);
        setupPermission("/a/bb", testPrincipal, false, PrivilegeConstants.JCR_READ);

        testRoot1 = getTestRoot();
        testRoot2 = testSession2.getLatestRoot();
    }


    @Test
    public void testAllowChild() throws Exception {
        Tree rootTree1 = testRoot1.getTree("/");
        Tree rootTree2 = testRoot2.getTree("/");

        assertFalse(rootTree1.hasChild("a/bb"));
        assertFalse(rootTree2.hasChild("a/bb"));

        // now allow read with root session
        setupPermission("/a/bb", testPrincipal, true, PrivilegeConstants.JCR_READ);

        // the test sessions still need to see the old ACLs
        assertFalse(rootTree1.hasChild("a/bb"));
        assertFalse(rootTree2.hasChild("a/bb"));
    }

    @Test
    public void testAllowChild2() throws Exception {
        // same as above, but before reading the items
        setupPermission("/a/bb", testPrincipal, true, PrivilegeConstants.JCR_READ);

        Tree rootTree1 = testRoot1.getTree("/");
        Tree rootTree2 = testRoot2.getTree("/");

        assertFalse(rootTree1.hasChild("a/bb"));
        assertFalse(rootTree2.hasChild("a/bb"));
    }

    @Test
    public void testAllowChild3() throws Exception {
        Tree rootTree1 = testRoot1.getTree("/");
        Tree rootTree2 = testRoot2.getTree("/");

        assertTrue(rootTree1.hasChild("a"));
        assertTrue(rootTree2.hasChild("a"));
        assertFalse(rootTree1.hasChild("a/bb"));
        assertFalse(rootTree2.hasChild("a/bb"));

        setupPermission(testRoot1, "/a", testPrincipal, false, PrivilegeConstants.JCR_READ);

        assertFalse(rootTree1.hasChild("a"));
        assertTrue(rootTree2.hasChild("a"));

        String uid = testPrincipal.getName();
        ContentSession session3 = login(new SimpleCredentials(uid, uid.toCharArray()));
        Tree rootTree3 = session3.getLatestRoot().getTree("/");
        assertFalse(rootTree3.hasChild("a"));
    }
}
