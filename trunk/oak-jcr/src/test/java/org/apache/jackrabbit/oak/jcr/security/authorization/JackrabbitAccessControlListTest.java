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

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import javax.jcr.Node;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.test.api.security.AbstractAccessControlTest;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

/**
 * Testing {@code JackrabbitAccessControlList} functionality exposed by the API.
 */
public class JackrabbitAccessControlListTest extends AbstractAccessControlTest {

    private JackrabbitAccessControlList acl;
    private Principal testPrincipal;
    private Privilege[] testPrivileges;

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Node n = testRootNode.addNode(nodeName1, testNodeType);
        superuser.save();

        AccessControlPolicyIterator it = acMgr.getApplicablePolicies(n.getPath());
        while (it.hasNext() && acl == null) {
            AccessControlPolicy p = it.nextAccessControlPolicy();
            if (p instanceof JackrabbitAccessControlList) {
                acl = (JackrabbitAccessControlList) p;
            }
        }
        if (acl == null) {
            superuser.logout();
            throw new NotExecutableException("No JackrabbitAccessControlList to test.");
        }

        testPrincipal = getValidPrincipal();
        testPrivileges = privilegesFromName(Privilege.JCR_ALL);
    }

    @Override
    protected void tearDown() throws Exception {
        // make sure transient ac-changes are reverted.
        superuser.refresh(false);
        super.tearDown();
    }

    private Principal getValidPrincipal() throws NotExecutableException, RepositoryException {
        if (!(superuser instanceof JackrabbitSession)) {
            throw new NotExecutableException();
        }

        PrincipalManager pMgr = ((JackrabbitSession) superuser).getPrincipalManager();
        return pMgr.getEveryone();
    }

    @Test
    public void testGetRestrictionNames() throws RepositoryException {
        assertNotNull(acl.getRestrictionNames());
    }

    @Test
    public void testGetRestrictionType() throws RepositoryException {
        String[] names = acl.getRestrictionNames();
        for (String name : names) {
            int type = acl.getRestrictionType(name);
            assertTrue(type > PropertyType.UNDEFINED);
        }
    }

    @Test
    public void testApplicablePolicyIsEmpty() {
        assertTrue(acl.isEmpty());
        assertEquals(0, acl.size());
    }

    @Test
    public void testIsEmpty() throws RepositoryException {
        if (acl.addAccessControlEntry(testPrincipal, testPrivileges)) {
            assertFalse(acl.isEmpty());
        } else {
            assertTrue(acl.isEmpty());
        }
    }

    @Test
    public void testSize() throws RepositoryException {
        if (acl.addAccessControlEntry(testPrincipal, testPrivileges)) {
            assertTrue(acl.size() > 0);
        } else {
            assertEquals(0, acl.size());
        }
    }

    @Test
    public void testAddEntry() throws NotExecutableException, RepositoryException {
        List<AccessControlEntry> entriesBefore = Arrays.asList(acl.getAccessControlEntries());
        if (acl.addEntry(testPrincipal, testPrivileges, true, Collections.<String, Value>emptyMap())) {
            AccessControlEntry[] entries = acl.getAccessControlEntries();
            AccessControlEntry ace = null;
            for (AccessControlEntry entry : entries) {
                if (testPrincipal.equals(entry.getPrincipal())) {
                    ace = entry;
                }
            }
            assertNotNull("addEntry was successful -> expected entry for tesPrincipal.", ace);
            assertTrue("addEntry was successful -> at least 1 entry.", entries.length > 0);
        } else {
            AccessControlEntry[] entries = acl.getAccessControlEntries();
            assertEquals("Grant ALL not successful -> entries must not have changed.", entriesBefore, Arrays.asList(entries));
        }
    }

    @Test
    public void testRemoveEntry() throws NotExecutableException, RepositoryException {
        Principal princ = getValidPrincipal();
        Privilege[] grPriv = privilegesFromName("rep:write");

        acl.addEntry(princ, grPriv, true, Collections.<String, Value>emptyMap());
        AccessControlEntry[] entries = acl.getAccessControlEntries();
        int length = entries.length;
        assertTrue("Grant was both successful -> at least 1 entry.", length > 0);
        for (AccessControlEntry entry : entries) {
            acl.removeAccessControlEntry(entry);
            length = length - 1;
            assertEquals(length, acl.size());
            assertEquals(length, acl.getAccessControlEntries().length);
        }

        assertTrue(acl.isEmpty());
        assertEquals(0, acl.size());
        assertEquals(0, acl.getAccessControlEntries().length);
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-1026">OAK-1026</a>
     */
    @Test
    public void testEntryWithAggregatePrivileges() throws Exception {
        Privilege write = acMgr.privilegeFromName(Privilege.JCR_WRITE);
        acl.addEntry(testPrincipal, write.getAggregatePrivileges(), true);

        AccessControlEntry[] entries = acl.getAccessControlEntries();
        assertEquals(1, entries.length);
        assertArrayEquals(new Privilege[]{write}, entries[0].getPrivileges());

        acMgr.setPolicy(acl.getPath(), acl);

        AccessControlPolicy policy = AccessControlUtils.getAccessControlList(acMgr, acl.getPath());
        assertNotNull(policy);

        entries = acl.getAccessControlEntries();
        assertEquals(1, entries.length);
        assertArrayEquals(new Privilege[]{write}, entries[0].getPrivileges());
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-1348">OAK-1348</a>
     */
    @Test
    public void testAddEntryWithCustomPrincipalImpl() throws Exception {
        Principal custom = new Principal() {
            public String getName() {
                return testPrincipal.getName();
            }
        };
        acl.addEntry(testPrincipal, testPrivileges, true);
        acl.addEntry(custom, testPrivileges, false);
        acMgr.setPolicy(acl.getPath(), acl);
        superuser.save();

    }
}