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
package org.apache.jackrabbit.oak.jcr.security.authorization;

import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.Workspace;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitWorkspace;
import org.apache.jackrabbit.test.NotExecutableException;
import org.junit.Ignore;
import org.junit.Test;

/**
 * PrivilegeManagementTest... TODO
 *
 * copied from jr2x RepositoryOperationTest
 */
@Ignore("OAK-51")
public class PrivilegeManagementTest extends AbstractEvaluationTest {

    private static final String REP_PRIVILEGE_MANAGEMENT = "rep:privilegeManagement";

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        // test user must not be allowed
        assertPrivilege(null, REP_PRIVILEGE_MANAGEMENT, false);
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            for (AccessControlPolicy policy : acMgr.getPolicies(null)) {
                acMgr.removePolicy(null, policy);
            }
            superuser.save();
        } finally {
            super.tearDown();
        }
    }

    private String getNewPrivilegeName(Workspace wsp) throws RepositoryException, NotExecutableException {
        String privName = null;
        AccessControlManager acMgr = wsp.getSession().getAccessControlManager();
        for (int i = 0; i < 100; i++) {
            try {
                Privilege p = acMgr.privilegeFromName(privName);
                privName = "privilege-" + i;
            } catch (Exception e) {
                break;
            }
        }

        if (privName == null) {
            throw new NotExecutableException("failed to define new privilege name.");
        }
        return privName;
    }

    @Test
    public void testRegisterPrivilege() throws Exception {
        try {
            Workspace testWsp = getTestSession().getWorkspace();
            ((JackrabbitWorkspace) testWsp).getPrivilegeManager().registerPrivilege(getNewPrivilegeName(testWsp), false, new String[0]);
            fail("Privilege registration should be denied.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testRegisterPrivilegeWithPrivilege() throws Exception {
        modify(null, REP_PRIVILEGE_MANAGEMENT, true);
        assertPrivilege(null, REP_PRIVILEGE_MANAGEMENT, true);

        try {
            Workspace testWsp = getTestSession().getWorkspace();
            ((JackrabbitWorkspace) testWsp).getPrivilegeManager().registerPrivilege(getNewPrivilegeName(testWsp), false, new String[0]);        } finally {
            modify(null, REP_PRIVILEGE_MANAGEMENT, false);
        }

        assertPrivilege(null, REP_PRIVILEGE_MANAGEMENT, false);
    }
}