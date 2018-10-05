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

import java.util.Set;
import javax.jcr.AccessDeniedException;
import javax.jcr.PathNotFoundException;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.Privilege;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepoLevelPolicyTest extends AbstractOakCoreTest implements PrivilegeConstants {

    @Test(expected = PathNotFoundException.class)
    public void testGetApplicablePoliciesRootNotReadable() throws Exception {
        setupPermission(null, getTestUser().getPrincipal(), true, JCR_READ_ACCESS_CONTROL);

        getAccessControlManager(getTestRoot()).getApplicablePolicies((String) null);
    }

    @Test(expected = PathNotFoundException.class)
    public void testGetApplicablePoliciesRootNotReadable2() throws Exception {
        setupPermission(null, getTestUser().getPrincipal(), true, JCR_READ, JCR_READ_ACCESS_CONTROL);

        getAccessControlManager(getTestRoot()).getApplicablePolicies((String) null);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetApplicablePoliciesMissingAcPermission() throws Exception {
        setupPermission(PathUtils.ROOT_PATH, getTestUser().getPrincipal(), true, JCR_READ);

        getAccessControlManager(getTestRoot()).getApplicablePolicies((String) null);
    }

    @Test(expected = AccessDeniedException.class)
    public void testGetApplicablePoliciesMissingAcPermission2() throws Exception {
        setupPermission(PathUtils.ROOT_PATH, getTestUser().getPrincipal(), true, JCR_READ, JCR_READ_ACCESS_CONTROL);

        getAccessControlManager(getTestRoot()).getApplicablePolicies((String) null);
    }

    @Test
    public void testGetApplicablePolicies() throws Exception {
        setupPermission(PathUtils.ROOT_PATH, getTestUser().getPrincipal(), true, JCR_READ);
        setupPermission(null, getTestUser().getPrincipal(), true, JCR_READ_ACCESS_CONTROL);

        getAccessControlManager(getTestRoot()).getApplicablePolicies((String) null);
    }

    @Test(expected = AccessDeniedException.class)
    public void testSetPolicyMissingAcPermission() throws Exception {
        setupPermission(PathUtils.ROOT_PATH, getTestUser().getPrincipal(), true, JCR_READ);
        setupPermission(null, getTestUser().getPrincipal(), true, JCR_READ_ACCESS_CONTROL);

        setupPermission(getTestRoot(), null, EveryonePrincipal.getInstance(), false, JCR_NAMESPACE_MANAGEMENT);
    }

    @Test(expected = AccessDeniedException.class)
    public void testSetPolicyMissingAcPermission2() throws Exception {
        setupPermission(PathUtils.ROOT_PATH, getTestUser().getPrincipal(), true, JCR_READ, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        setupPermission(null, getTestUser().getPrincipal(), true, JCR_READ_ACCESS_CONTROL);

        setupPermission(getTestRoot(), null, EveryonePrincipal.getInstance(), false, JCR_NAMESPACE_MANAGEMENT);
    }

    @Test
    public void testSetPolicy() throws Exception {
        setupPermission(PathUtils.ROOT_PATH, getTestUser().getPrincipal(), true, JCR_READ);
        setupPermission(null, getTestUser().getPrincipal(), true, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);

        setupPermission(getTestRoot(), null, EveryonePrincipal.getInstance(), false, JCR_NAMESPACE_MANAGEMENT);
    }

    @Test
    public void testSetPolicy2() throws Exception {
        // see above: ac-related permissions should not be required on ROOT_PATH (workaround for OAK-5947)
        setupPermission(PathUtils.ROOT_PATH, getTestUser().getPrincipal(), true, JCR_READ, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);
        setupPermission(null, getTestUser().getPrincipal(), true, JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL);

        setupPermission(getTestRoot(), null, EveryonePrincipal.getInstance(), false, JCR_NAMESPACE_MANAGEMENT);
    }

    @Test
    public void testHasPrivilege() throws Exception {
        setupPermission(null, getTestUser().getPrincipal(), true, JCR_READ_ACCESS_CONTROL, JCR_NAMESPACE_MANAGEMENT);

        AccessControlManager testAcMgr = getAccessControlManager(getTestRoot());
        assertTrue(testAcMgr.hasPrivileges(null, privilegesFromNames(JCR_NAMESPACE_MANAGEMENT)));
        assertTrue(testAcMgr.hasPrivileges(null, privilegesFromNames(JCR_READ_ACCESS_CONTROL)));
        assertTrue(testAcMgr.hasPrivileges(null, privilegesFromNames(JCR_READ_ACCESS_CONTROL, JCR_NAMESPACE_MANAGEMENT)));
        assertFalse(testAcMgr.hasPrivileges(null, privilegesFromNames(JCR_READ_ACCESS_CONTROL, JCR_MODIFY_ACCESS_CONTROL)));
        assertFalse(testAcMgr.hasPrivileges(null, privilegesFromNames(JCR_ALL)));
    }

    @Test
    public void testGetPrivileges() throws Exception {
        setupPermission(null, getTestUser().getPrincipal(), true, JCR_READ_ACCESS_CONTROL, JCR_NAMESPACE_MANAGEMENT);

        Set<Privilege> expected = ImmutableSet.copyOf(privilegesFromNames(JCR_READ_ACCESS_CONTROL, JCR_NAMESPACE_MANAGEMENT));

        AccessControlManager testAcMgr = getAccessControlManager(getTestRoot());
        assertEquals(expected, ImmutableSet.copyOf(testAcMgr.getPrivileges(null)));
    }
}