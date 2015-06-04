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

import java.security.Principal;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.principal.AdminPrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.principal.SystemPrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

public abstract class AdminPrincipalsBaseTest extends AbstractSecurityTest {

    static final String ADMINISTRATORS_PRINCIPAL_NAME = "administrators";

    AccessControlList acl;
    Principal administrativePrincipal;

    @Override
    public void before() throws Exception {
        super.before();

        NodeUtil rootNode = new NodeUtil(root.getTree("/"), NamePathMapper.DEFAULT);
        rootNode.addChild("testNode", JcrConstants.NT_UNSTRUCTURED);

        administrativePrincipal = getUserManager(root).createGroup(new PrincipalImpl(ADMINISTRATORS_PRINCIPAL_NAME)).getPrincipal();
        root.commit();

        AccessControlManager acMgr = getAccessControlManager(root);
        AccessControlPolicyIterator itr = acMgr.getApplicablePolicies("/testNode");
        while (itr.hasNext() && acl == null) {
            AccessControlPolicy policy = itr.nextAccessControlPolicy();
            if (policy instanceof AccessControlList)  {
                acl = (AccessControlList) policy;
            }
        }

        if (acl == null) {
            throw new RepositoryException("No applicable policy found.");
        }
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            root.getTree("/testNode").remove();

            Authorizable gr = getUserManager(root).getAuthorizable(administrativePrincipal);
            if (gr != null) {
                gr.remove();
            }
            root.commit();
        } finally {
            super.after();
        }
    }

    abstract void assertResult(boolean success) throws Exception;
    abstract void assertException() throws Exception;

    /**
     * Test if the ACL code properly deals the creation of ACEs for administrative
     * principals which have full access anyway.
     *
     * @since Oak 1.1.1
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2158">OAK-2158</a>
     */
    @Test
    public void testAdminPrincipal() throws Exception {
        try {
            boolean success = acl.addAccessControlEntry(new AdminPrincipal() {
                @Override
                public String getName() {
                    return "admin";
                }
            }, privilegesFromNames(PrivilegeConstants.JCR_READ));
            assertResult(success);
        } catch (AccessControlException e) {
            assertException();
        }
    }

    @Test
    public void testAdminAuthInfoPrincipals() throws Exception {
        try {
            for (Principal p : adminSession.getAuthInfo().getPrincipals()) {
                if (p instanceof AdminPrincipal) {
                    boolean success = acl.addAccessControlEntry(p, privilegesFromNames(PrivilegeConstants.JCR_READ));
                    assertResult(success);
                }
            }
        } catch (AccessControlException e) {
            assertException();
        }
    }

    /**
     * Test if the ACL code properly deals the creation of ACEs for system
     * principals which have full access anyway.
     *
     * @since Oak 1.3.0
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2955">OAK-2955</a>
     */
    @Test
    public void testSystemPrincipal() throws Exception {
        try {
            boolean success = acl.addAccessControlEntry(SystemPrincipal.INSTANCE, privilegesFromNames(PrivilegeConstants.JCR_READ));
            assertResult(success);
        } catch (AccessControlException e) {
            assertException();
        }
    }

    /**
     * Test if the ACL code properly deals the creation of ACEs for configured
     * admin-principals, which have full access anyway.
     *
     * @since Oak 1.3.0
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2955">OAK-2955</a>
     */
    @Test
    public void testConfiguredAdministrativePrincipal() throws Exception {
        try {
            boolean success = acl.addAccessControlEntry(administrativePrincipal, privilegesFromNames(PrivilegeConstants.JCR_READ));
            assertResult(success);
        } catch (AccessControlException e) {
            assertException();
        }
    }
}