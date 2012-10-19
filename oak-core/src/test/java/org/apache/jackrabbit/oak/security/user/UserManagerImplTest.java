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
package org.apache.jackrabbit.oak.security.user;

import java.util.ArrayList;
import java.util.List;

import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.security.AbstractSecurityTest;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtility;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.fail;

/**
 * UserManagerImplTest...
 */
public class UserManagerImplTest extends AbstractSecurityTest {

    ContentSession admin;
    Root root;
    UserManagerImpl userMgr;

    @Before
    public void before() throws Exception {
        super.before();
        admin = login(getAdminCredentials());
        root = admin.getLatestRoot();
        userMgr = (UserManagerImpl) new UserConfigurationImpl(ConfigurationParameters.EMPTY, securityProvider).getUserManager(root, NamePathMapper.DEFAULT);
    }

    @After
    public void after() throws Exception {
        admin.close();
    }

    @Test
    public void testSetPassword() throws Exception {
        User user = userMgr.createUser("a", "pw");
        root.commit();

        List<String> pwds = new ArrayList<String>();
        pwds.add("pw");
        pwds.add("");
        pwds.add("{sha1}pw");

        Tree userTree = root.getTree(user.getPath());
        for (String pw : pwds) {
            userMgr.setPassword(userTree, pw, true);
            String pwHash = userTree.getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
            assertNotNull(pwHash);
            assertTrue(PasswordUtility.isSame(pwHash, pw));
        }

        for (String pw : pwds) {
            userMgr.setPassword(userTree, pw, false);
            String pwHash = userTree.getProperty(UserConstants.REP_PASSWORD).getValue(Type.STRING);
            assertNotNull(pwHash);
            if (!pw.startsWith("{")) {
                assertTrue(PasswordUtility.isSame(pwHash, pw));
            } else {
                assertFalse(PasswordUtility.isSame(pwHash, pw));
                assertEquals(pw, pwHash);
            }
        }
    }

    @Test
    public void setPasswordNull() throws Exception {
        User user = userMgr.createUser("a", null);
        root.commit();

        Tree userTree = root.getTree(user.getPath());
        try {
            userMgr.setPassword(userTree, null, true);
            fail("setting null password should fail");
        } catch (IllegalArgumentException e) {
            // expected
        }

        try {
            userMgr.setPassword(userTree, null, false);
            fail("setting null password should fail");
        } catch (IllegalArgumentException e) {
            // expected
        }
    }

    @Test
    public void testGetPasswordHash() throws Exception {
        User user = userMgr.createUser("a", null);
        root.commit();

        Tree userTree = root.getTree(user.getPath());
        assertNull(userTree.getProperty(UserConstants.REP_PASSWORD));
    }
}