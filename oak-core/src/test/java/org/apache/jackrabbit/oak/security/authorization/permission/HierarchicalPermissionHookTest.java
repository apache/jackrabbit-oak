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
package org.apache.jackrabbit.oak.security.authorization.permission;

import java.security.Principal;
import java.util.Arrays;
import java.util.Collections;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlManager;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.commons.jackrabbit.authorization.AccessControlUtils;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.security.privilege.PrivilegeBits;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Testing the {@code PermissionHook}
 */
@Ignore() // TODO: to be removed
public class HierarchicalPermissionHookTest extends AbstractPermissionHookTest {

    @Test
    public void testReorderForSinglePrincipal() throws Exception {
        Principal testPrincipal = getTestPrincipal();
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_MODIFY_ACCESS_CONTROL), false);
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_READ_ACCESS_CONTROL), true, Collections.singletonMap(REP_GLOB, getValueFactory().createValue("/*")));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        /*
        Original setup with 3 access control entries for testPrincipal @ testPath
        Expected result:
        0 - testuser - allow - JCR_ADD_CHILD_NODES       - NA
        1 - everyone - allow - READ                      - NA
        2 - testuser - deny  - JCR_MODIFY_ACCESS_CONTROL - NA
        3 - testuser - allow - JCR_READ_ACCESS_CONTROL   - /*
        */
        long rootDepths = PathUtils.getDepth(getPrincipalRoot(testPrincipalName).getPath());
        assertEntry(getEntry(testPrincipalName, testPath, 0), bitsProvider.getBits(JCR_ADD_CHILD_NODES), true, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 2), bitsProvider.getBits(JCR_MODIFY_ACCESS_CONTROL), false, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 3), bitsProvider.getBits(JCR_READ_ACCESS_CONTROL), true, ++rootDepths);

        /*
        Reorder entries
        Expected result:
        0 - allow - JCR_ADD_CHILD_NODES       - NA
        1 - everyone - allow - READ                      - NA
        2 - allow - JCR_READ_ACCESS_CONTROL   - /*
        3 - deny  - JCR_MODIFY_ACCESS_CONTROL - NA
        */
        acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            if (testPrincipal.equals(ace.getPrincipal()) && Arrays.equals(privilegesFromNames(JCR_MODIFY_ACCESS_CONTROL), ace.getPrivileges())) {
                acl.orderBefore(ace, null);
            }
        }
        acMgr.setPolicy(testPath, acl);
        root.commit();

        rootDepths = PathUtils.getDepth(getPrincipalRoot(testPrincipalName).getPath());
        assertEntry(getEntry(testPrincipalName, testPath, 0), bitsProvider.getBits(JCR_ADD_CHILD_NODES), true, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 2), bitsProvider.getBits(JCR_READ_ACCESS_CONTROL), true, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 3), bitsProvider.getBits(JCR_MODIFY_ACCESS_CONTROL), false, ++rootDepths);

        /*
        Remove all entries
        */
        acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            if (testPrincipal.equals(ace.getPrincipal())) {
                acl.removeAccessControlEntry(ace);
            }
        }
        acMgr.setPolicy(testPath, acl);
        root.commit();

        assertEquals(0, cntEntries(getPrincipalRoot(testPrincipalName)));
    }

    @Test
    public void testReorderForSinglePrincipal2() throws Exception {
        Principal testPrincipal = getTestPrincipal();
        AccessControlManager acMgr = getAccessControlManager(root);
        JackrabbitAccessControlList acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_MODIFY_ACCESS_CONTROL), false);
        acl.addEntry(testPrincipal, privilegesFromNames(JCR_READ_ACCESS_CONTROL), true, Collections.singletonMap(REP_GLOB, getValueFactory().createValue("/*")));
        acMgr.setPolicy(testPath, acl);
        root.commit();

        /*
        Original setup with 3 access control entries for testPrincipal @ testPath
        Expected result:
        0 - testuser - allow - JCR_ADD_CHILD_NODES       - NA
        1 - everyone - allow - READ                      - NA
        2 - testuser - deny  - JCR_MODIFY_ACCESS_CONTROL - NA
        3 - testuser - allow - JCR_READ_ACCESS_CONTROL   - /*
        */
        long rootDepths = PathUtils.getDepth(getPrincipalRoot(testPrincipalName).getPath());
        assertEntry(getEntry(testPrincipalName, testPath, 0), bitsProvider.getBits(JCR_ADD_CHILD_NODES), true, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 2), bitsProvider.getBits(JCR_MODIFY_ACCESS_CONTROL), false, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 3), bitsProvider.getBits(JCR_READ_ACCESS_CONTROL), true, ++rootDepths);

        /*
        Reorder entries
        Expected result:
        0 - allow - JCR_READ_ACCESS_CONTROL   - /*
        1 - allow - JCR_ADD_CHILD_NODES       - NA
        3 - deny  - JCR_MODIFY_ACCESS_CONTROL - NA
        */
        acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        AccessControlEntry first = null;
        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            if (testPrincipal.equals(ace.getPrincipal())) {
                if (first == null) {
                    first = ace;
                }
                if (Arrays.equals(privilegesFromNames(JCR_READ_ACCESS_CONTROL), ace.getPrivileges())) {
                    acl.orderBefore(ace, first);
                }
            }
        }
        acMgr.setPolicy(testPath, acl);
        root.commit();

        rootDepths = PathUtils.getDepth(getPrincipalRoot(testPrincipalName).getPath());
        assertEntry(getEntry(testPrincipalName, testPath, 0), bitsProvider.getBits(JCR_READ_ACCESS_CONTROL), true, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 1), bitsProvider.getBits(JCR_ADD_CHILD_NODES), true, ++rootDepths);
        assertEntry(getEntry(testPrincipalName, testPath, 3), bitsProvider.getBits(JCR_MODIFY_ACCESS_CONTROL), false, ++rootDepths);

        /*
        Remove all entries
        */
        acl = AccessControlUtils.getAccessControlList(acMgr, testPath);
        for (AccessControlEntry ace : acl.getAccessControlEntries()) {
            if (testPrincipal.equals(ace.getPrincipal())) {
                acl.removeAccessControlEntry(ace);
            }
        }
        acMgr.setPolicy(testPath, acl);
        root.commit();

        assertEquals(0, cntEntries(getPrincipalRoot(testPrincipalName)));
    }

    private static void assertEntry(Tree entry, PrivilegeBits expectedBits, boolean isAllow, long expectedDepth) {
        assertEquals(expectedBits, PrivilegeBits.getInstance(entry.getProperty(REP_PRIVILEGE_BITS)));
        assertEquals(isAllow, entry.getProperty(REP_IS_ALLOW).getValue(Type.BOOLEAN));
        assertEquals(expectedDepth, PathUtils.getDepth(entry.getPath()));
    }
}