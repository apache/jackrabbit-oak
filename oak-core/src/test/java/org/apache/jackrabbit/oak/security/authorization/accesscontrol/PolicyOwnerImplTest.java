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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlPolicy;

import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.permission.PermissionConstants;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PolicyOwnerImplTest extends AbstractAccessControlTest {

    private AccessControlManagerImpl acMgr;

    @Override
    @Before
    public void before() throws Exception {
        super.before();

        acMgr = new AccessControlManagerImpl(root, getNamePathMapper(), getSecurityProvider());

        AccessControlList policy = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        policy.addAccessControlEntry(testPrincipal, testPrivileges);
        acMgr.setPolicy(TEST_PATH, policy);

        root.commit();
    }

    @Test
    public void testDefines() throws Exception {
        assertTrue(acMgr.defines(TEST_PATH, AccessControlUtils.getAccessControlList(acMgr, TEST_PATH)));
    }

    @Test
    public void testDefinesReadPolicy() throws Exception {
        String readPath = PermissionConstants.DEFAULT_READ_PATHS.iterator().next();
        assertTrue(acMgr.defines(readPath, AccessControlUtils.getAccessControlList(acMgr, readPath)));
    }


    @Test
    public void testDefinesWrongPath() throws Exception {
        String readPath = PermissionConstants.DEFAULT_READ_PATHS.iterator().next();

        assertFalse(acMgr.defines(PathUtils.ROOT_PATH, AccessControlUtils.getAccessControlList(acMgr, TEST_PATH)));
        assertFalse(acMgr.defines(TEST_PATH, AccessControlUtils.getAccessControlList(acMgr, readPath)));
    }

    @Test
    public void testDefinesDifferentPolicy() throws Exception {
        assertFalse(acMgr.defines(TEST_PATH, new AccessControlPolicy() {}));
    }

    @Test
    public void testDefinesWithRelPath() throws Exception {
        assertFalse(acMgr.defines("testPath", AccessControlUtils.getAccessControlList(acMgr, TEST_PATH)));
    }
}