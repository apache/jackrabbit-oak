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

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.authorization.AuthorizationConfiguration;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.apache.jackrabbit.oak.spi.xml.ProtectedItemImporter;
import org.jetbrains.annotations.NotNull;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlException;
import javax.jcr.security.AccessControlPolicy;
import java.security.Principal;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants.JCR_READ;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@RunWith(Parameterized.class)
public class AccessControlWithUnknownPrincipalTest extends AbstractAccessControlTest {

    @Parameterized.Parameters(name = "ImportBehavior={1}")
    public static Collection<Object[]> parameters() {
        return List.of(
                new Object[] {ImportBehavior.IGNORE , ImportBehavior.NAME_IGNORE},
                new Object[] {ImportBehavior.BESTEFFORT, ImportBehavior.NAME_BESTEFFORT},
                new Object[] {ImportBehavior.ABORT, ImportBehavior.NAME_ABORT}
        );
    }
    
    private final int importBehavior;
    private final String importBehaviorName;

    private AccessControlManagerImpl acMgr;

    public AccessControlWithUnknownPrincipalTest(int importBehavior, String importBehaviorName) {
        this.importBehavior = importBehavior;
        this.importBehaviorName = importBehaviorName;
    }
    
    @Before
    public void before() throws Exception {
        super.before();

        acMgr = new AccessControlManagerImpl(root, getNamePathMapper(), getSecurityProvider());
    }

    @Override
    protected ConfigurationParameters getSecurityConfigParameters() {
        ConfigurationParameters params = ConfigurationParameters.of(ProtectedItemImporter.PARAM_IMPORT_BEHAVIOR, importBehaviorName);
        return ConfigurationParameters.of(AuthorizationConfiguration.NAME, params);
    }
    
    @NotNull
    private String getUnknownPrincipalName() {
        Principal unknown = getPrincipalManager(root).getPrincipal("unknown");
        int i = 0;
        while (unknown != null) {
            unknown = getPrincipalManager(root).getPrincipal("unknown"+i);
        }
        return "unknown"+i;
    }
    
    private void assertImportBehavior(String message) {
        // success if importBehavior == ABORT
        if (importBehavior != ImportBehavior.ABORT) {
            fail(message);
        }
    }

    @Test
    public void testGetApplicablePoliciesInvalidPrincipal() throws Exception {
        Principal unknown = new InvalidTestPrincipal(getUnknownPrincipalName());
        try {
            AccessControlPolicy[] applicable = acMgr.getApplicablePolicies(unknown);
            switch (importBehavior) {
                case ImportBehavior.IGNORE:
                    assertEquals(0, applicable.length);
                    break;
                case ImportBehavior.BESTEFFORT:
                    assertEquals(1, applicable.length);
                    break;
                case ImportBehavior.ABORT:
                default:
                    fail("Getting applicable policies for unknown principal should fail");
            } 
        } catch (AccessControlException e) {
            assertImportBehavior("Getting applicable policies for unknown principal with importBehavior "+importBehaviorName+" must not throw AccessControlException");
        }
    }

    @Test
    public void testGetApplicablePoliciesInternalPrincipal() throws Exception {
        Principal unknown = new PrincipalImpl(getUnknownPrincipalName());
        assertPolicies(acMgr.getApplicablePolicies(unknown), 1);
    }

    @Test
    public void testGetPoliciesInvalidPrincipal() throws Exception {
        Principal unknown = new InvalidTestPrincipal(getUnknownPrincipalName());
        assertGetPolicies(unknown, 0);
    }
    
    @Test
    public void testGetPoliciesRemovedPrincipal() throws Exception {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        assertNotNull(acl);
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_READ), true);
        acMgr.setPolicy(TEST_PATH, acl);
        
        Principal p = () -> testPrincipal.getName();
        removeTestUser();

        assertGetPolicies(p, 1);
    }
    
    private void assertGetPolicies(@NotNull Principal principal, int expectedBestEffort) throws Exception {
        try {
            AccessControlPolicy[] policies = acMgr.getPolicies(principal);
            switch (importBehavior) {
                case ImportBehavior.IGNORE:
                    assertEquals(0, policies.length);
                    break;
                case ImportBehavior.BESTEFFORT:
                    assertEquals(expectedBestEffort, policies.length);
                    break;
                case ImportBehavior.ABORT:
                default:
                    fail("Getting applicable policies for unknown principal should fail");
            }
        } catch (AccessControlException e) {
            assertImportBehavior("Getting policies for unknown principal with importBehavior "+importBehaviorName+" must not throw AccessControlException");
        }
    }
    
    @Test
    public void testGetPoliciesInternalPrincipal() throws Exception {
        // internal principal implementation is allowed irrespective of import-mode
        Principal unknown = new PrincipalImpl(getUnknownPrincipalName());
        assertPolicies(acMgr.getPolicies(unknown), 0);
    }

    @Test
    public void testGetEffectivePoliciesInvalidPrincipal() throws Exception {
        Principal unknown = new InvalidTestPrincipal(getUnknownPrincipalName());
        try {
            AccessControlPolicy[] effective = acMgr.getEffectivePolicies(Collections.singleton(unknown));
            switch (importBehavior) {
                case ImportBehavior.IGNORE:
                    assertPolicies(effective, 0, false);
                    break;
                case ImportBehavior.BESTEFFORT:
                    assertPolicies(effective, 1, true);
                    break;
                case ImportBehavior.ABORT:
                default:
                    fail("Getting effective policies for unknown principal should fail");
            }
        } catch (AccessControlException e) {
            assertImportBehavior("Getting effective policies for unknown principal with importBehavior "+importBehaviorName+" must not throw AccessControlException");
        }
    }

    @Test
    public void testGetEffectivePoliciesInternalPrincipal() throws Exception {
        Principal unknown = new PrincipalImpl(getUnknownPrincipalName());
        AccessControlPolicy[] effective = acMgr.getEffectivePolicies(Collections.singleton(unknown));
        assertPolicies(effective, 1, true);
    }

    @Test
    public void testAddEntryInvalidPrincipal() throws Exception {
        Principal unknownPrincipal = new InvalidTestPrincipal("unknown");
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        try {
            boolean modified = acl.addAccessControlEntry(unknownPrincipal, privilegesFromNames(JCR_READ));
            switch (importBehavior) {
                case ImportBehavior.IGNORE: 
                    assertFalse(modified);
                    break;
                case ImportBehavior.BESTEFFORT: 
                    assertTrue(modified);
                    break;
                case ImportBehavior.ABORT:
                default:
                    fail("Adding an ACE with an unknown principal should fail");
            }
        } catch (AccessControlException e) {
            assertImportBehavior("Adding entry for unknown principal with importBehavior "+importBehaviorName+" must not throw AccessControlException");
        }
    }

    @Test
    public void testAddEntryInternalPrincipal() throws RepositoryException {
        // internal principal impl is allowed irrespective of import mode
        Principal internal = new PrincipalImpl("unknown");
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        boolean modified = acl.addAccessControlEntry(internal, privilegesFromNames(JCR_READ));
        assertTrue(modified);
    }
    
    @Test(expected = AccessControlException.class)
    public void testNullPrincipal() throws Exception {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        acl.addAccessControlEntry(null, privilegesFromNames(JCR_READ));
    }
    
    @Test(expected = AccessControlException.class)
    public void testEmptyPrincipal() throws Exception {
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, TEST_PATH);
        acl.addAccessControlEntry(new PrincipalImpl(""), privilegesFromNames(JCR_READ));
    }
}