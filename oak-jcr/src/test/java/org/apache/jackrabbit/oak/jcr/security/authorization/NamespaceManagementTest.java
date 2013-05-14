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

import java.util.Arrays;
import java.util.List;
import javax.annotation.Nonnull;
import javax.jcr.AccessDeniedException;
import javax.jcr.RepositoryException;
import javax.jcr.Workspace;
import javax.jcr.security.AccessControlPolicy;

import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Permission evaluation tests related to {@link PrivilegeConstants#JCR_NAMESPACE_MANAGEMENT} privilege.
 */
public class NamespaceManagementTest extends AbstractEvaluationTest {

    @Override
    @Before
    protected void setUp() throws Exception {
        super.setUp();

        assertHasRepoPrivilege(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT, false);
    }

    @Override
    @After
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

    @Nonnull
    private String getNewNamespacePrefix(Workspace wsp) throws RepositoryException {
        String prefix = "prefix";
        List<String> pfcs = Arrays.asList(wsp.getNamespaceRegistry().getPrefixes());
        int i = 0;
        while (pfcs.contains(prefix)) {
            prefix = "prefix" + i++;
        }
        return prefix;
    }

    @Nonnull
    private String getNewNamespaceURI(Workspace wsp) throws RepositoryException {
        String uri = "http://jackrabbit.apache.org/uri";
        List<String> uris = Arrays.asList(wsp.getNamespaceRegistry().getURIs());
        int i = 0;
        while (uris.contains(uri)) {
            uri = "http://jackrabbit.apache.org/uri_" + i++;
        }
        return uri;
    }

    @Test
    public void testRegisterNamespace() throws Exception {
        try {
            Workspace testWsp = testSession.getWorkspace();
            testWsp.getNamespaceRegistry().registerNamespace(getNewNamespacePrefix(testWsp), getNewNamespaceURI(testWsp));
            fail("Namespace registration should be denied.");
        } catch (AccessDeniedException e) {
            // success
        }
    }

    @Test
    public void testModifyNamespaceWithPrivilege() throws Exception {
        modify(null, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT.toString(), true);
        assertHasRepoPrivilege(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT, true);

        modify(null, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT.toString(), false);
        assertHasRepoPrivilege(PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT, false);
    }

    @Test
    public void testRegisterNamespaceWithPrivilege() throws Exception {
        modify(null, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT.toString(), true);
        try {
            Workspace testWsp = testSession.getWorkspace();
            testWsp.getNamespaceRegistry().registerNamespace(getNewNamespacePrefix(testWsp), getNewNamespaceURI(testWsp));
        } finally {
            modify(null, PrivilegeConstants.JCR_NAMESPACE_MANAGEMENT.toString(), false);
        }
    }

    @Test
    public void testUnregisterNamespace() throws Exception {
        Workspace wsp = superuser.getWorkspace();
        String pfx = getNewNamespacePrefix(wsp);
        wsp.getNamespaceRegistry().registerNamespace(pfx, getNewNamespaceURI(wsp));

        try {
            Workspace testWsp = testSession.getWorkspace();
            testWsp.getNamespaceRegistry().unregisterNamespace(pfx);
            fail("Namespace unregistration should be denied.");
        } catch (AccessDeniedException e) {
            // success
        } finally {
            // clean up (not supported by jackrabbit-core)
            try {
                superuser.getWorkspace().getNamespaceRegistry().unregisterNamespace(pfx);
            } catch (Exception e) {
                // ns unregistration is not supported by jackrabbit-core.
            }
        }
    }
}