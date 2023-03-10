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
package org.apache.jackrabbit.oak.spi.security.authorization.principalbased.impl;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.ImmutableACL;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.privilege.PrivilegeConstants;
import org.junit.Before;
import org.junit.Test;

import javax.jcr.security.AccessControlException;
import java.util.Collections;

import static org.apache.jackrabbit.oak.spi.security.authorization.accesscontrol.AccessControlConstants.REP_GLOB;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class ImmutablePrincipalPolicyTest extends AbstractPrincipalBasedTest {

    private PrincipalPolicyImpl policy;
    private ImmutablePrincipalPolicy immutable;

    @Before
    @Override
    public void before() throws Exception {
        super.before();

        policy = setupPrincipalBasedAccessControl(getTestSystemUser().getPrincipal(), testContentJcrPath, PrivilegeConstants.JCR_READ);
        immutable = new ImmutablePrincipalPolicy(policy);
    }

    @Test
    public void testGetPrincipal() throws Exception {
        assertEquals(getTestSystemUser().getPrincipal(), immutable.getPrincipal());
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntry() throws Exception {
        immutable.addEntry(PathUtils.ROOT_PATH, privilegesFromNames(PrivilegeConstants.JCR_READ));
    }

    @Test(expected = AccessControlException.class)
    public void testAddEntryWithRestrictions() throws Exception {
        immutable.addEntry(null, privilegesFromNames(PrivilegeConstants.JCR_ALL), Collections.singletonMap(getNamePathMapper().getJcrName(REP_GLOB), getValueFactory(root).createValue("*")), Collections.emptyMap());
    }

    @Test
    public void testHashcode() {
        int expectedHashCode = immutable.hashCode();
        ImmutablePrincipalPolicy ipp = new ImmutablePrincipalPolicy(policy.getPrincipal(), policy.getOakPath(), policy.getEntries(), policy.getRestrictionProvider(), policy.getNamePathMapper());
        assertEquals(expectedHashCode, ipp.hashCode());
        assertEquals(expectedHashCode, new ImmutablePrincipalPolicy(policy).hashCode());
    }

    @Test
    public void testEquals() {
        ImmutablePrincipalPolicy ipp = new ImmutablePrincipalPolicy(policy.getPrincipal(), policy.getOakPath(), policy.getEntries(), policy.getRestrictionProvider(), policy.getNamePathMapper());
        assertEquals(immutable, ipp);
        assertEquals(immutable, new ImmutablePrincipalPolicy(policy));
        assertEquals(immutable, immutable);
    }

    @Test
    public void testNotEquals() {
        ImmutablePrincipalPolicy differentPath = new ImmutablePrincipalPolicy(policy.getPrincipal(), "/different/path", policy.getEntries(), policy.getRestrictionProvider(), policy.getNamePathMapper());
        ImmutablePrincipalPolicy differentPrincipal = new ImmutablePrincipalPolicy(EveryonePrincipal.getInstance(), policy.getOakPath(), policy.getEntries(), policy.getRestrictionProvider(), policy.getNamePathMapper());
        ImmutablePrincipalPolicy differentEntries = new ImmutablePrincipalPolicy(policy.getPrincipal(), policy.getOakPath(), Collections.emptyList(), policy.getRestrictionProvider(), policy.getNamePathMapper());

        assertNotEquals(immutable, policy);
        assertNotEquals(immutable, new ImmutableACL(policy));
        assertNotEquals(immutable, differentPath);
        assertNotEquals(immutable, differentPrincipal);
        assertNotEquals(immutable, differentEntries);

        int hc = immutable.hashCode();
        assertNotEquals(hc, policy.hashCode());
        assertNotEquals(hc, new ImmutableACL(policy).hashCode());
        assertNotEquals(hc, differentPath.hashCode());
        assertNotEquals(hc, differentPrincipal.hashCode());
        assertNotEquals(hc, differentEntries.hashCode());
    }
}