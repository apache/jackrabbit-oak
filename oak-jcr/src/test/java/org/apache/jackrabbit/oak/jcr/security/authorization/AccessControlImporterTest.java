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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import javax.jcr.ImportUUIDBehavior;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.security.AccessControlEntry;
import javax.jcr.security.AccessControlList;
import javax.jcr.security.AccessControlManager;
import javax.jcr.security.AccessControlPolicy;
import javax.jcr.security.AccessControlPolicyIterator;
import javax.jcr.security.Privilege;

import org.apache.jackrabbit.api.security.JackrabbitAccessControlEntry;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlList;
import org.apache.jackrabbit.oak.jcr.SessionImpl;
import org.apache.jackrabbit.test.AbstractJCRTest;
import org.xml.sax.SAXException;

public class AccessControlImporterTest extends AbstractJCRTest {

    private static final String XML_POLICY_TREE   = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"test\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                    "<sv:value>nt:unstructured</sv:value>" +
                "</sv:property>" +
                "<sv:property sv:name=\"jcr:mixinTypes\" sv:type=\"Name\">" +
                    "<sv:value>rep:AccessControllable</sv:value>" +
                    "<sv:value>mix:versionable</sv:value>" +
                "</sv:property>" +
                "<sv:property sv:name=\"jcr:uuid\" sv:type=\"String\">" +
                    "<sv:value>0a0ca2e9-ab98-4433-a12b-d57283765207</sv:value>" +
                "</sv:property>" +
                "<sv:property sv:name=\"jcr:baseVersion\" sv:type=\"Reference\">" +
                    "<sv:value>35d0d137-a3a4-4af3-8cdd-ce565ea6bdc9</sv:value>" +
                "</sv:property>" +
                "<sv:property sv:name=\"jcr:isCheckedOut\" sv:type=\"Boolean\">" +
                    "<sv:value>true</sv:value>" +
                "</sv:property>" +
                "<sv:property sv:name=\"jcr:predecessors\" sv:type=\"Reference\">" +
                    "<sv:value>35d0d137-a3a4-4af3-8cdd-ce565ea6bdc9</sv:value>" +
                "</sv:property>" +
                "<sv:property sv:name=\"jcr:versionHistory\" sv:type=\"Reference\">" +
                    "<sv:value>428c9ef2-78e5-4f1c-95d3-16b4ce72d815</sv:value>" +
                "</sv:property>" +
                "<sv:node sv:name=\"rep:policy\">" +
                    "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                        "<sv:value>rep:ACL</sv:value>" +
                    "</sv:property>" +
                    "<sv:node sv:name=\"allow\">" +
                        "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                            "<sv:value>rep:GrantACE</sv:value>" +
                        "</sv:property>" +
                        "<sv:property sv:name=\"rep:principalName\" sv:type=\"String\">" +
                            "<sv:value>everyone</sv:value>" +
                        "</sv:property>" +
                        "<sv:property sv:name=\"rep:privileges\" sv:type=\"Name\">" +
                            "<sv:value>jcr:write</sv:value>" +
                        "</sv:property>" +
                    "</sv:node>" +
                "</sv:node>" +
            "</sv:node>";

    private static final String XML_POLICY_TREE_2 = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"rep:policy\" " +
            "xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                    "<sv:value>rep:ACL</sv:value>" +
                "</sv:property>" +
                "<sv:node sv:name=\"allow\">" +
                    "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                        "<sv:value>rep:GrantACE</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:principalName\" sv:type=\"String\">" +
                        "<sv:value>everyone</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:privileges\" sv:type=\"Name\">" +
                        "<sv:value>jcr:write</sv:value>" +
                    "</sv:property>" +
                "</sv:node>" +
            "</sv:node>";

    private static final String XML_POLICY_TREE_3   = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"rep:policy\" " +
                    "xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                    "<sv:value>rep:ACL</sv:value>" +
                "</sv:property>" +
                "<sv:node sv:name=\"allow\">" +
                    "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                        "<sv:value>rep:GrantACE</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:principalName\" sv:type=\"String\">" +
                        "<sv:value>everyone</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:privileges\" sv:type=\"Name\">" +
                        "<sv:value>jcr:write</sv:value>" +
                    "</sv:property>" +
                "</sv:node>" +
                "<sv:node sv:name=\"allow0\">" +
                    "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                        "<sv:value>rep:GrantACE</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:principalName\" sv:type=\"String\">" +
                        "<sv:value>admin</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:privileges\" sv:type=\"Name\">" +
                        "<sv:value>jcr:write</sv:value>" +
                    "</sv:property>" +
                "</sv:node>" +
            "</sv:node>";

    private static final String XML_POLICY_TREE_4   = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"rep:policy\" " +
                    "xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                    "<sv:value>rep:ACL</sv:value>" +
                "</sv:property>" +
                "<sv:node sv:name=\"allow\">" +
                    "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                        "<sv:value>rep:GrantACE</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:principalName\" sv:type=\"String\">" +
                        "<sv:value>unknownprincipal</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:privileges\" sv:type=\"Name\">" +
                        "<sv:value>jcr:write</sv:value>" +
                    "</sv:property>" +
                "</sv:node>" +
                "<sv:node sv:name=\"allow0\">" +
                    "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                        "<sv:value>rep:GrantACE</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:principalName\" sv:type=\"String\">" +
                        "<sv:value>admin</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:privileges\" sv:type=\"Name\">" +
                        "<sv:value>jcr:write</sv:value>" +
                    "</sv:property>" +
                "</sv:node>" +
            "</sv:node>";

    private static final String XML_POLICY_TREE_5   = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"rep:policy\" " +
                    "xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                    "<sv:value>rep:ACL</sv:value>" +
                "</sv:property>" +
                "<sv:node sv:name=\"allow0\">" +
                    "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                        "<sv:value>rep:GrantACE</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:principalName\" sv:type=\"String\">" +
                        "<sv:value>admin</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:privileges\" sv:type=\"Name\">" +
                        "<sv:value>jcr:write</sv:value>" +
                    "</sv:property>" +
                "</sv:node>" +
            "</sv:node>";

    private static final String XML_REPO_POLICY_TREE = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<sv:node sv:name=\"rep:repoPolicy\" " +
                    "xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\">" +
                "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                    "<sv:value>rep:ACL</sv:value>" +
                "</sv:property>" +
                "<sv:node sv:name=\"allow\">" +
                    "<sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\">" +
                        "<sv:value>rep:GrantACE</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:principalName\" sv:type=\"String\">" +
                        "<sv:value>admin</sv:value>" +
                    "</sv:property>" +
                    "<sv:property sv:name=\"rep:privileges\" sv:type=\"Name\">" +
                        "<sv:value>jcr:workspaceManagement</sv:value>" +
                    "</sv:property>" +
                "</sv:node>" +
            "</sv:node>";

    private static final String XML_AC_TREE       = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><sv:node sv:name=\"rep:security\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:crx=\"http://www.day.com/crx/1.0\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:repl=\"http://www.day.com/crx/replication/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AccessControl</sv:value></sv:property><sv:node sv:name=\"rep:authorizables\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AccessControl</sv:value></sv:property><sv:node sv:name=\"rep:groups\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AccessControl</sv:value></sv:property><sv:node sv:name=\"administrators\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:PrincipalAccessControl</sv:value></sv:property><sv:node sv:name=\"rep:policy\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:ACL</sv:value></sv:property><sv:node sv:name=\"entry\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:GrantACE</sv:value></sv:property><sv:property sv:name=\"rep:glob\" sv:type=\"String\"><sv:value>*</sv:value></sv:property><sv:property sv:name=\"rep:nodePath\" sv:type=\"Path\"><sv:value>/</sv:value></sv:property><sv:property sv:name=\"rep:principalName\" sv:type=\"String\"><sv:value>administrators</sv:value></sv:property><sv:property sv:name=\"rep:privileges\" sv:type=\"Name\"><sv:value>jcr:all</sv:value></sv:property></sv:node></sv:node></sv:node></sv:node><sv:node sv:name=\"rep:users\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AccessControl</sv:value></sv:property><sv:node sv:name=\"admin\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:AccessControl</sv:value></sv:property><sv:node sv:name=\"t\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:PrincipalAccessControl</sv:value></sv:property></sv:node><sv:node sv:name=\"a\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:PrincipalAccessControl</sv:value></sv:property></sv:node></sv:node><sv:node sv:name=\"anonymous\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:PrincipalAccessControl</sv:value></sv:property></sv:node></sv:node></sv:node></sv:node>";

    private static final String XML_POLICY_ONLY   = "<?xml version=\"1.0\" encoding=\"UTF-8\"?><sv:node sv:name=\"test\" xmlns:mix=\"http://www.jcp.org/jcr/mix/1.0\" xmlns:nt=\"http://www.jcp.org/jcr/nt/1.0\" xmlns:fn_old=\"http://www.w3.org/2004/10/xpath-functions\" xmlns:fn=\"http://www.w3.org/2005/xpath-functions\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:sv=\"http://www.jcp.org/jcr/sv/1.0\" xmlns:rep=\"internal\" xmlns:jcr=\"http://www.jcp.org/jcr/1.0\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>nt:unstructured</sv:value></sv:property><sv:property sv:name=\"jcr:mixinTypes\" sv:type=\"Name\"><sv:value>rep:AccessControllable</sv:value><sv:value>mix:versionable</sv:value></sv:property><sv:property sv:name=\"jcr:uuid\" sv:type=\"String\"><sv:value>0a0ca2e9-ab98-4433-a12b-d57283765207</sv:value></sv:property><sv:property sv:name=\"jcr:baseVersion\" sv:type=\"Reference\"><sv:value>35d0d137-a3a4-4af3-8cdd-ce565ea6bdc9</sv:value></sv:property><sv:property sv:name=\"jcr:isCheckedOut\" sv:type=\"Boolean\"><sv:value>true</sv:value></sv:property><sv:property sv:name=\"jcr:predecessors\" sv:type=\"Reference\"><sv:value>35d0d137-a3a4-4af3-8cdd-ce565ea6bdc9</sv:value></sv:property><sv:property sv:name=\"jcr:versionHistory\" sv:type=\"Reference\"><sv:value>428c9ef2-78e5-4f1c-95d3-16b4ce72d815</sv:value></sv:property><sv:node sv:name=\"rep:policy\"><sv:property sv:name=\"jcr:primaryType\" sv:type=\"Name\"><sv:value>rep:ACL</sv:value></sv:property></sv:node></sv:node>";

    /**
     * Imports a resource-based ACL containing a single entry.
     *
     * @throws Exception
     */
    public void testImportACL() throws Exception {
        try {
            Node target = testRootNode;
            doImport(target.getPath(), XML_POLICY_TREE);

            assertTrue(target.hasNode("test"));
            String path = target.getNode("test").getPath();

            AccessControlManager acMgr = superuser.getAccessControlManager();
            AccessControlPolicy[] policies = acMgr.getPolicies(path);

            assertEquals(1, policies.length);
            assertTrue(policies[0] instanceof JackrabbitAccessControlList);

            AccessControlEntry[] entries = ((JackrabbitAccessControlList) policies[0]).getAccessControlEntries();
            assertEquals(1, entries.length);

            AccessControlEntry entry = entries[0];
            assertEquals("everyone", entry.getPrincipal().getName());
            assertEquals(1, entry.getPrivileges().length);
            assertEquals(acMgr.privilegeFromName(Privilege.JCR_WRITE), entry.getPrivileges()[0]);

            if(entry instanceof JackrabbitAccessControlEntry) {
                assertTrue(((JackrabbitAccessControlEntry) entry).isAllow());
            }

        } finally {
            superuser.refresh(false);
        }
    }

    /**
     * Imports a resource-based ACL containing a single entry.
     *
     * @throws Exception
     */
    public void testImportACLOnly() throws Exception {
        try {
            Node target = testRootNode.addNode(nodeName1);
            target.addMixin("rep:AccessControllable");

            doImport(target.getPath(), XML_POLICY_TREE_3);

            String path = target.getPath();

            AccessControlManager acMgr = superuser.getAccessControlManager();
            AccessControlPolicy[] policies = acMgr.getPolicies(path);

            assertEquals(1, policies.length);
            assertTrue(policies[0] instanceof JackrabbitAccessControlList);

            AccessControlEntry[] entries = ((JackrabbitAccessControlList) policies[0]).getAccessControlEntries();
            assertEquals(2, entries.length);

            AccessControlEntry entry = entries[0];
            assertEquals("everyone", entry.getPrincipal().getName());
            assertEquals(1, entry.getPrivileges().length);
            assertEquals(acMgr.privilegeFromName(Privilege.JCR_WRITE), entry.getPrivileges()[0]);

            entry = entries[1];
            assertEquals("admin", entry.getPrincipal().getName());
            assertEquals(1, entry.getPrivileges().length);
            assertEquals(acMgr.privilegeFromName(Privilege.JCR_WRITE), entry.getPrivileges()[0]);

            if(entry instanceof JackrabbitAccessControlEntry) {
                assertTrue(((JackrabbitAccessControlEntry) entry).isAllow());
            }
        } finally {
            superuser.refresh(false);
        }
    }

    /**
     * Imports a resource-based ACL containing a single entry.
     *
     * @throws Exception
     */
    public void testImportACLRemoveACE() throws Exception {
        try {
            Node target = testRootNode.addNode(nodeName1);
            target.addMixin("rep:AccessControllable");

            doImport(target.getPath(), XML_POLICY_TREE_3);
            doImport(target.getPath(), XML_POLICY_TREE_5);

            String path = target.getPath();

            AccessControlManager acMgr = superuser.getAccessControlManager();
            AccessControlPolicy[] policies = acMgr.getPolicies(path);

            assertEquals(1, policies.length);
            assertTrue(policies[0] instanceof JackrabbitAccessControlList);

            AccessControlEntry[] entries = ((JackrabbitAccessControlList) policies[0]).getAccessControlEntries();
            //FIXME assert fails
            assertEquals(1, entries.length);

            AccessControlEntry entry = entries[0];
            assertEquals("admin", entry.getPrincipal().getName());
            assertEquals(1, entry.getPrivileges().length);
            assertEquals(acMgr.privilegeFromName(Privilege.JCR_WRITE), entry.getPrivileges()[0]);

            if(entry instanceof JackrabbitAccessControlEntry) {
                assertTrue(((JackrabbitAccessControlEntry) entry).isAllow());
            }
        } finally {
            superuser.refresh(false);
        }
    }

    /**
     * Imports a resource-based ACL containing a single entry.
     *
     * @throws Exception
     */
    public void testImportACLUnknown() throws Exception {
        try {
            Node target = testRootNode.addNode(nodeName1);
            target.addMixin("rep:AccessControllable");

            //FIXME import fails
            doImport(target.getPath(), XML_POLICY_TREE_4);

            String path = target.getPath();

            AccessControlManager acMgr = superuser.getAccessControlManager();
            AccessControlPolicy[] policies = acMgr.getPolicies(path);

            assertEquals(1, policies.length);
            assertTrue(policies[0] instanceof JackrabbitAccessControlList);

            AccessControlEntry[] entries = ((JackrabbitAccessControlList) policies[0]).getAccessControlEntries();
            assertEquals(2, entries.length);

            AccessControlEntry entry = entries[0];
            assertEquals("unknownprincipal", entry.getPrincipal().getName());
            assertEquals(1, entry.getPrivileges().length);
            assertEquals(acMgr.privilegeFromName(Privilege.JCR_WRITE), entry.getPrivileges()[0]);

            entry = entries[1];
            assertEquals("admin", entry.getPrincipal().getName());
            assertEquals(1, entry.getPrivileges().length);
            assertEquals(acMgr.privilegeFromName(Privilege.JCR_WRITE), entry.getPrivileges()[0]);

            if(entry instanceof JackrabbitAccessControlEntry) {
                assertTrue(((JackrabbitAccessControlEntry) entry).isAllow());
            }
        } finally {
            superuser.refresh(false);
        }
    }

    /**
     * Imports a resource-based ACL containing a single entry for a policy that
     * already exists.
     *
     * @throws Exception
     */
    public void testImportPolicyExists() throws Exception {
        // this test does not work anymore, since the normal behavior is replace
        // all ACEs for an import. maybe control this behavior via uuid-flag.
        if (true) {
            return;
        }

        Node target = testRootNode;
        target = target.addNode("test", "test:sameNameSibsFalseChildNodeDefinition");
        AccessControlManager acMgr = superuser.getAccessControlManager();
        for (AccessControlPolicyIterator it = acMgr.getApplicablePolicies(target.getPath()); it.hasNext();) {
            AccessControlPolicy policy = it.nextAccessControlPolicy();
            if (policy instanceof AccessControlList) {
                Privilege[] privs = new Privilege[] {acMgr.privilegeFromName(Privilege.JCR_LOCK_MANAGEMENT)};
                ((AccessControlList) policy).addAccessControlEntry(((SessionImpl)superuser).getPrincipalManager().getEveryone(), privs);
                acMgr.setPolicy(target.getPath(), policy);
            }
        }

        try {

            doImport(target.getPath(), XML_POLICY_TREE_2);

            AccessControlPolicy[] policies = acMgr.getPolicies(target.getPath());

            assertEquals(1, policies.length);
            assertTrue(policies[0] instanceof JackrabbitAccessControlList);

            AccessControlEntry[] entries = ((JackrabbitAccessControlList) policies[0]).getAccessControlEntries();
            assertEquals(1, entries.length);

            AccessControlEntry entry = entries[0];
            assertEquals("everyone", entry.getPrincipal().getName());
            List<Privilege> privs = Arrays.asList(entry.getPrivileges());
            assertEquals(2, privs.size());
            assertTrue(privs.contains(acMgr.privilegeFromName(Privilege.JCR_WRITE)) &&
                    privs.contains(acMgr.privilegeFromName(Privilege.JCR_LOCK_MANAGEMENT)));

            assertEquals(acMgr.privilegeFromName(Privilege.JCR_WRITE), entry.getPrivileges()[0]);

            if(entry instanceof JackrabbitAccessControlEntry) {
                assertTrue(((JackrabbitAccessControlEntry) entry).isAllow());
            }

        } finally {
            superuser.refresh(false);
        }
    }

    /**
     * Imports an empty resource-based ACL for a policy that already exists.
     *
     * @throws Exception
     */
    public void testImportEmptyExistingPolicy() throws Exception {
        Node target = testRootNode;
        target = target.addNode("test", "test:sameNameSibsFalseChildNodeDefinition");
        AccessControlManager acMgr = superuser.getAccessControlManager();
        for (AccessControlPolicyIterator it = acMgr.getApplicablePolicies(target.getPath()); it.hasNext();) {
            AccessControlPolicy policy = it.nextAccessControlPolicy();
            if (policy instanceof AccessControlList) {
                acMgr.setPolicy(target.getPath(), policy);
            }
        }

        try {
            doImport(target.getPath(), XML_POLICY_ONLY);

            AccessControlPolicy[] policies = acMgr.getPolicies(target.getPath());

            assertEquals(1, policies.length);
            assertTrue(policies[0] instanceof JackrabbitAccessControlList);

            AccessControlEntry[] entries = ((JackrabbitAccessControlList) policies[0]).getAccessControlEntries();
            assertEquals(0, entries.length);

        } finally {
            superuser.refresh(false);
        }
    }

    /**
     * Repo level acl must be imported underneath the root node.
     *
     * @throws Exception
     */
    public void testImportRepoACLAtRoot() throws Exception {
        Node target = superuser.getRootNode();
        AccessControlManager acMgr = superuser.getAccessControlManager();
        try {
            // need to add mixin. in contrast to only using JCR API to retrieve
            // and set the policies the protected item import only is called if
            // the node to be imported is defined to be protected. however, if
            // the root node doesn't have the mixin assigned the defining node
            // type of the imported policy nodes will be rep:root (unstructured)
            // and the items will not be detected as being protected.
            target.addMixin("rep:RepoAccessControllable");

            doImport(target.getPath(), XML_REPO_POLICY_TREE);

            AccessControlPolicy[] policies = acMgr.getPolicies(null);

            assertEquals(1, policies.length);
            assertTrue(policies[0] instanceof JackrabbitAccessControlList);

            AccessControlEntry[] entries = ((JackrabbitAccessControlList) policies[0]).getAccessControlEntries();
            assertEquals(1, entries.length);
            assertEquals(1, entries[0].getPrivileges().length);
            assertEquals(acMgr.privilegeFromName("jcr:workspaceManagement"), entries[0].getPrivileges()[0]);

            assertTrue(target.hasNode("rep:repoPolicy"));
            assertTrue(target.hasNode("rep:repoPolicy/allow"));

            // clean up again
            acMgr.removePolicy(null, policies[0]);
            assertFalse(target.hasNode("rep:repoPolicy"));
            assertFalse(target.hasNode("rep:repoPolicy/allow"));

        } finally {
            superuser.refresh(false);
        }
    }

    /**
     * Make sure repo-level acl is not imported below any other node than the
     * root node.
     *
     * @throws Exception
     */
    public void testImportRepoACLAtTestNode() throws Exception {
        Node target = testRootNode.addNode("test");
        target.addMixin("rep:RepoAccessControllable");

        AccessControlManager acMgr = superuser.getAccessControlManager();
        try {
            doImport(target.getPath(), XML_REPO_POLICY_TREE);

            AccessControlPolicy[] policies = acMgr.getPolicies(null);
            assertEquals(0, policies.length);

            assertTrue(target.hasNode("rep:repoPolicy"));
            assertFalse(target.hasNode("rep:repoPolicy/allow0"));

            Node n = target.getNode("rep:repoPolicy");
            assertEquals("rep:RepoAccessControllable", n.getDefinition().getDeclaringNodeType().getName());
        } finally {
            superuser.refresh(false);
        }
    }

    /**
     * Imports a principal-based ACL containing a single entry mist fail with
     * the default configuration.
     *
     * @throws Exception
     */
    public void testImportPrincipalBasedACL() throws Exception {
//        JackrabbitAccessControlManager acMgr = (JackrabbitAccessControlManager) superuser.getAccessControlManager();
//        if (acMgr.getApplicablePolicies(EveryonePrincipal.getInstance()).length > 0 ||
//                acMgr.getPolicies(EveryonePrincipal.getInstance()).length > 0) {
//            // test expects that only resource-based acl is supported
//            throw new NotExecutableException();
//        }
//
//        PrincipalManager pmgr = sImpl.getPrincipalManager();
//        if (!pmgr.hasPrincipal(SecurityConstants.ADMINISTRATORS_NAME)) {
//            UserManager umgr = sImpl.getUserManager();
//            umgr.createGroup(new PrincipalImpl(SecurityConstants.ADMINISTRATORS_NAME));
//            if (!umgr.isAutoSave()) {
//                sImpl.save();
//            }
//            if (pmgr.hasPrincipal(SecurityConstants.ADMINISTRATORS_NAME)) {
//                throw new NotExecutableException();
//            }
//        }
//
//
//        NodeImpl target;
//        NodeImpl root = (NodeImpl) sImpl.getRootNode();
//        if (!root.hasNode(AccessControlConstants.N_ACCESSCONTROL)) {
//            target = root.addNode(AccessControlConstants.N_ACCESSCONTROL, AccessControlConstants.NT_REP_ACCESS_CONTROL, null);
//        } else {
//            target = root.getNode(AccessControlConstants.N_ACCESSCONTROL);
//            if (!target.isNodeType(AccessControlConstants.NT_REP_ACCESS_CONTROL)) {
//                target.setPrimaryType(sImpl.getJCRName(AccessControlConstants.NT_REP_ACCESS_CONTROL));
//            }
//        }
//        try {
//
//            InputStream in = new ByteArrayInputStream(XML_AC_TREE.getBytes("UTF-8"));
//
//            SessionImporter importer = new SessionImporter(target, sImpl, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW, new PseudoConfig());
//            ImportHandler ih = new ImportHandler(importer, sImpl);
//            new ParsingContentHandler(ih).parse(in);
//
//            fail("Default config only allows resource-based ACL -> protected import must fail");
//
//        } catch (SAXException e) {
//            if (e.getException() instanceof ConstraintViolationException) {
//                // success
//            } else {
//                throw e;
//            }
//        } finally {
//            superuser.refresh(false);
//        }
    }

    private void doImport(String parentPath, String xml) throws IOException, SAXException, RepositoryException {
        InputStream in = new ByteArrayInputStream(xml.getBytes("UTF-8"));
        superuser.importXML(parentPath, in, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW);
    }
}
