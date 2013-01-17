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
import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.test.NotExecutableException;
import org.apache.jackrabbit.test.api.security.AbstractAccessControlTest;

/**
 * JackrabbitAccessControlListTest... TODO
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
        PrincipalIterator it = pMgr.getPrincipals(PrincipalManager.SEARCH_TYPE_NOT_GROUP);
        if (it.hasNext()) {
            return it.nextPrincipal();
        } else {
            throw new NotExecutableException();
        }
    }

    public void testGetRestrictionNames() throws RepositoryException {
        assertNotNull(acl.getRestrictionNames());
    }

    public void testGetRestrictionType() throws RepositoryException {
        String[] names = acl.getRestrictionNames();
        for (String name : names) {
            int type = acl.getRestrictionType(name);
            assertTrue(type > PropertyType.UNDEFINED);
        }
    }

    public void testApplicablePolicyIsEmpty() {
        assertTrue(acl.isEmpty());
        assertEquals(0, acl.size());
    }

    public void testIsEmpty() throws RepositoryException {
        if (acl.addAccessControlEntry(testPrincipal, testPrivileges)) {
            assertFalse(acl.isEmpty());
        } else {
            assertTrue(acl.isEmpty());
        }
    }

    public void testSize() throws RepositoryException {
        if (acl.addAccessControlEntry(testPrincipal, testPrivileges)) {
            assertTrue(acl.size() > 0);
        } else {
            assertEquals(0, acl.size());
        }
    }

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

    // TODO: rewrite
//    public void testAllowWriteDenyRemove() throws NotExecutableException, RepositoryException {
//        Principal princ = getValidPrincipal();
//        Privilege[] grPriv = privilegesFromName("rep:write");
//        Privilege[] dePriv = privilegesFromName(Privilege.JCR_REMOVE_CHILD_NODES);
//
//        acl.addEntry(princ, grPriv, true, Collections.<String, Value>emptyMap());
//        acl.addEntry(princ, dePriv, false, Collections.<String, Value>emptyMap());
//
//        Set<Privilege> allows = new HashSet<Privilege>();
//        Set<Privilege> denies = new HashSet<Privilege>();
//        AccessControlEntry[] entries = acl.getAccessControlEntries();
//        for (AccessControlEntry en : entries) {
//            if (princ.equals(en.getPrincipal()) && en instanceof JackrabbitAccessControlEntry) {
//                JackrabbitAccessControlEntry ace = (JackrabbitAccessControlEntry) en;
//                Privilege[] privs = ace.getPrivileges();
//                if (ace.isAllow()) {
//                    allows.addAll(Arrays.asList(privs));
//                } else {
//                    denies.addAll(Arrays.asList(privs));
//                }
//            }
//        }
//
//        String[] expected = new String[] {Privilege.JCR_ADD_CHILD_NODES, Privilege.JCR_REMOVE_NODE, Privilege.JCR_MODIFY_PROPERTIES, Privilege.JCR_NODE_TYPE_MANAGEMENT};
//        assertEquals(expected.length, allows.size());
//        for (String name : expected) {
//            assertTrue(allows.contains(acMgr.privilegeFromName(name)));
//        }
//
//        assertEquals(1, denies.size());
//        assertEquals(acMgr.privilegeFromName(Privilege.JCR_REMOVE_CHILD_NODES), denies.iterator().next());
//    }

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
}