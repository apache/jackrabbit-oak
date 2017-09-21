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
package org.apache.jackrabbit.oak.security.user;

import javax.jcr.ImportUUIDBehavior;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.PropertyDefinition;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.AuthorizableExistsException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.util.PasswordUtil;
import org.apache.jackrabbit.oak.spi.xml.PropInfo;
import org.apache.jackrabbit.oak.spi.xml.ReferenceChangeTracker;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class UserImporterTest extends UserImporterBaseTest implements UserConstants {

    //---------------------------------------------------------------< init >---
    @Test
    public void testInitNoJackrabbitSession() throws Exception {
        Session s = Mockito.mock(Session.class);
        assertFalse(importer.init(s, root, getNamePathMapper(), false, ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test(expected = IllegalStateException.class)
    public void testInitAlreadyInitialized() throws Exception {
        init();
        importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider());
    }

    @Test
    public void testInitImportUUIDBehaviorRemove() throws Exception {
        assertTrue(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REMOVE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider()));
    }


    @Test
    public void testInitImportUUIDBehaviorReplace() throws Exception {
        assertTrue(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_REPLACE_EXISTING, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test
    public void testInitImportUUIDBehaviorThrow() throws Exception {
        assertTrue(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_COLLISION_THROW, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test
    public void testInitImportUUIDBehaviourCreateNew() throws Exception {
        assertFalse(importer.init(mockJackrabbitSession(), root, getNamePathMapper(), isWorkspaceImport(), ImportUUIDBehavior.IMPORT_UUID_CREATE_NEW, new ReferenceChangeTracker(), getSecurityProvider()));
    }

    @Test(expected = IllegalStateException.class)
    public void testHandlePropInfoNotInitialized() throws Exception {
        importer.handlePropInfo(createUserTree(), Mockito.mock(PropInfo.class), Mockito.mock(PropertyDefinition.class));
    }

    //-----------------------------------------------------< handlePropInfo >---

    @Test
    public void testHandlePropInfoParentNotAuthorizable() throws Exception {
        init();
        assertFalse(importer.handlePropInfo(root.getTree("/"), Mockito.mock(PropInfo.class), Mockito.mock(PropertyDefinition.class)));
    }

    @Test
    public void testHandleAuthorizableId() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, TEST_USER_ID), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false)));
        assertEquals(TEST_USER_ID, userTree.getProperty(REP_AUTHORIZABLE_ID).getValue(Type.STRING));
        assertEquals(userTree.getPath(), getUserManager(root).getAuthorizable(TEST_USER_ID).getPath());
    }

    @Test(expected = ConstraintViolationException.class)
    public void testHandleAuthorizableIdMismatch() throws Exception {
        init();
        Tree userTree = createUserTree();
        importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, "mismatch"), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false));
    }

    @Test(expected = AuthorizableExistsException.class)
    public void testHandleAuthorizableIdConflictExisting() throws Exception {
        init();
        Tree userTree = createUserTree();
        importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, testUser.getID()), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false));
    }

    @Test
    public void testHandleAuthorizableIdMvPropertyDef() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, TEST_USER_ID), mockPropertyDefinition(NT_REP_AUTHORIZABLE, true)));
        assertNull(userTree.getProperty(REP_AUTHORIZABLE_ID));
    }

    @Test
    public void testHandleAuthorizableIdOtherDeclNtDef() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, TEST_USER_ID), mockPropertyDefinition(NT_REP_AUTHORIZABLE_FOLDER, false)));
        assertNull(userTree.getProperty(REP_AUTHORIZABLE_ID));
    }

    @Test
    public void testHandleAuthorizableIdDeclNtDefSubtype() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, TEST_USER_ID), mockPropertyDefinition(NT_REP_USER, false)));
    }

    @Test
    public void testHandlePrincipalName() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_PRINCIPAL_NAME, "principalName"), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false)));
        assertEquals("principalName", userTree.getProperty(REP_PRINCIPAL_NAME).getValue(Type.STRING));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHandleEmptyPrincipalName() throws Exception {
        init();
        Tree userTree = createUserTree();
        importer.handlePropInfo(userTree, createPropInfo(REP_PRINCIPAL_NAME, ""), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testHandleEveryonePrincipalNameOnUser() throws Exception {
        init();
        Tree userTree = createUserTree();
        importer.handlePropInfo(userTree, createPropInfo(REP_PRINCIPAL_NAME, EveryonePrincipal.NAME), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false));
    }

    @Test
    public void testHandlePrincipalNameMvPropertyDef() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_PRINCIPAL_NAME, "principalName"), mockPropertyDefinition(NT_REP_AUTHORIZABLE, true)));
        assertNull(userTree.getProperty(REP_PRINCIPAL_NAME));
    }

    @Test
    public void testHandlePrincipalNameOtherDeclNtDef() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_PRINCIPAL_NAME, "principalName"), mockPropertyDefinition(NT_REP_AUTHORIZABLE_FOLDER, false)));
        assertNull(userTree.getProperty(REP_PRINCIPAL_NAME));
    }

    @Test
    public void testHandlePassword() throws Exception {
        init();
        Tree userTree = createUserTree();
        String pwHash = PasswordUtil.buildPasswordHash("pw");
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_PASSWORD, pwHash), mockPropertyDefinition(NT_REP_USER, false)));
        assertEquals(pwHash, userTree.getProperty(REP_PASSWORD).getValue(Type.STRING));
    }

    @Test
    public void testHandlePasswordOnSystemUser() throws Exception {
        init();
        Tree userTree = createUserTree();
        userTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_SYSTEM_USER, Type.NAME);
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_PASSWORD, PasswordUtil.buildPasswordHash("pw")), mockPropertyDefinition(NT_REP_USER, false)));
    }

    @Test
    public void testHandlePasswordOnGroup() throws Exception {
        init();
        Tree groupTree = createGroupTree();
        assertFalse(importer.handlePropInfo(groupTree, createPropInfo(REP_PASSWORD, PasswordUtil.buildPasswordHash("pw")), mockPropertyDefinition(NT_REP_USER, false)));
    }

    @Test
    public void testHandlePasswordMvPropertyDef() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_PASSWORD, PasswordUtil.buildPasswordHash("pw")), mockPropertyDefinition(NT_REP_USER, true)));
        assertNull(userTree.getProperty(REP_PASSWORD));
    }

    @Test
    public void testHandlePasswordOtherDeclNtDef() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_PASSWORD, PasswordUtil.buildPasswordHash("pw")), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false)));
        assertNull(userTree.getProperty(REP_PASSWORD));
    }

    @Test
    public void testHandleImpersonators() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, "impersonator1", "impersonator2"), mockPropertyDefinition(NT_REP_USER, true)));
        // writing is postponed though and the ref-tracker must not be empty
        assertNull(userTree.getProperty(REP_IMPERSONATORS));
        assertTrue(refTracker.getProcessedReferences().hasNext());
    }

    @Test
    public void testHandleImpersonatorsOnGroup() throws Exception {
        init();
        Tree groupTree = createGroupTree();
        assertFalse(importer.handlePropInfo(groupTree, createPropInfo(REP_IMPERSONATORS, "impersonator1"), mockPropertyDefinition(NT_REP_USER, true)));
    }

    @Test
    public void testHandleImpersonatorsSinglePropertyDef() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, "impersonator1"), mockPropertyDefinition(NT_REP_USER, false)));
        assertNull(userTree.getProperty(REP_IMPERSONATORS));
    }

    @Test
    public void testHandleImpersonatorsOtherDeclNtDef() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, "impersonator1"), mockPropertyDefinition(NT_REP_AUTHORIZABLE, true)));
        assertNull(userTree.getProperty(REP_IMPERSONATORS));
    }

    @Test
    public void testHandleMembers() throws Exception {
        init();
        Tree groupTree = createGroupTree();
        assertTrue(importer.handlePropInfo(groupTree, createPropInfo(REP_MEMBERS, "member1", "member2"), mockPropertyDefinition(NT_REP_MEMBER_REFERENCES, true)));
        // writing is postponed though
        assertNull(groupTree.getProperty(REP_MEMBERS));
    }

    @Test
    public void testHandleMembersOnUser() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_MEMBERS, "member1"), mockPropertyDefinition(NT_REP_MEMBER_REFERENCES, true)));
    }

    @Test
    public void testHandleMembersSinglePropertyDef() throws Exception {
        init();
        Tree groupTree = createGroupTree();
        assertFalse(importer.handlePropInfo(groupTree, createPropInfo(REP_MEMBERS, "member1"), mockPropertyDefinition(NT_REP_MEMBER_REFERENCES, false)));
        assertNull(groupTree.getProperty(REP_MEMBERS));
    }

    @Test
    public void testHandleMembersOtherDeclNtDef() throws Exception {
        init();
        Tree groupTree = createGroupTree();
        assertFalse(importer.handlePropInfo(groupTree, createPropInfo(REP_MEMBERS, "member1"), mockPropertyDefinition(NT_REP_AUTHORIZABLE, true)));
        assertNull(groupTree.getProperty(REP_MEMBERS));
    }

    @Test
    public void testHandleDisabled() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_DISABLED, "disabled"), mockPropertyDefinition(NT_REP_USER, false)));
        PropertyState property = userTree.getProperty(REP_DISABLED);
        assertNotNull(property);
        assertEquals("disabled", property.getValue(Type.STRING));
    }

    @Test
    public void testHandleDisabledOnGroup() throws Exception {
        init();
        Tree groupTree = createGroupTree();
        assertFalse(importer.handlePropInfo(groupTree, createPropInfo(REP_DISABLED, "disabled"), mockPropertyDefinition(NT_REP_USER, false)));
        assertNull(groupTree.getProperty(REP_DISABLED));
    }

    @Test(expected = RepositoryException.class)
    public void testHandleDisabledMvProperty() throws Exception {
        init();
        Tree userTree = createUserTree();
        importer.handlePropInfo(userTree, createPropInfo(REP_DISABLED, "disabled", "disabled"), mockPropertyDefinition(NT_REP_USER, false));
    }

    @Test
    public void testHandleDisabledMvPropertyDef() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_DISABLED, "disabled"), mockPropertyDefinition(NT_REP_USER, true)));
        assertNull(userTree.getProperty(REP_DISABLED));
    }

    @Test
    public void testHandleDisabledOtherDeclNtDef() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo(REP_DISABLED, "disabled"), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false)));
        assertNull(userTree.getProperty(REP_DISABLED));
    }

    @Test
    public void testHandleUnknownProperty() throws Exception {
        init();
        Tree userTree = createUserTree();
        assertFalse(importer.handlePropInfo(userTree, createPropInfo("unknownProperty", "value"), mockPropertyDefinition(NodeTypeConstants.NT_OAK_UNSTRUCTURED, false)));
        assertNull(userTree.getProperty("unknownProperty"));
    }

    //--------------------------------------------------< processReferences >---

    @Test(expected = IllegalStateException.class)
    public void testProcessReferencesNotInitialized() throws Exception {
        importer.processReferences();
    }

    //------------------------------------------------< propertiesCompleted >---

    @Test
    public void testPropertiesCompletedClearsCache() throws Exception {
        Tree userTree = createUserTree();
        Tree cacheTree = userTree.addChild(CacheConstants.REP_CACHE);
        cacheTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, CacheConstants.NT_REP_CACHE);

        importer.propertiesCompleted(cacheTree);
        assertFalse(cacheTree.exists());
        assertFalse(userTree.hasChild(CacheConstants.REP_CACHE));
    }

    @Test
    public void testPropertiesCompletedParentNotAuthorizable() throws Exception {
        init();
        importer.propertiesCompleted(root.getTree("/"));
    }

    @Test
    public void testPropertiesCompletedIdMissing() throws Exception {
        init();
        Tree userTree = createUserTree();
        importer.propertiesCompleted(userTree);

        assertTrue(userTree.hasProperty(REP_AUTHORIZABLE_ID));
    }

    @Test
    public void testPropertiesCompletedIdPresent() throws Exception {
        init();
        testAction = new TestAction();

        Tree userTree = createUserTree();
        userTree.setProperty(REP_AUTHORIZABLE_ID, "userId");

        importer.propertiesCompleted(userTree);

        // property must not be touched
        assertEquals("userId", userTree.getProperty(REP_AUTHORIZABLE_ID).getValue(Type.STRING));
    }

    @Test
    public void testPropertiesCompletedNewUser() throws Exception {
        init(true);
        importer.propertiesCompleted(createUserTree());
        testAction.checkMethods("onCreate-User");
    }

    @Test
    public void testPropertiesCompletedNewSystemUser() throws Exception {
        init(true);
        importer.propertiesCompleted(createSystemUserTree());
        // create-actions must not be called for system users
        testAction.checkMethods();
    }

    @Test
    public void testPropertiesCompletedNewGroup() throws Exception {
        Tree groupTree = createGroupTree();

        init(true);
        importer.propertiesCompleted(groupTree);
        testAction.checkMethods("onCreate-Group");
    }

    @Test
    public void testPropertiesCompletedExistingUser() throws Exception {
        init(true);
        importer.propertiesCompleted(root.getTree(testUser.getPath()));
        testAction.checkMethods();
    }

    //--------------------------------------------------------------< start >---
    @Test
    public void testStartUserTree() throws Exception {
        init(true);
        assertFalse(importer.start(createUserTree()));
    }

    @Test
    public void testStartGroupTree() throws Exception {
        init(true);
        assertFalse(importer.start(createGroupTree()));
    }

    @Test
    public void testStartMembersRefListTree() throws Exception {
        init(true);
        Tree groupTree = createGroupTree();
        Tree memberRefList = groupTree.addChild(REP_MEMBERS_LIST);
        memberRefList.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBER_REFERENCES_LIST);

        assertTrue(importer.start(memberRefList));
    }

    @Test
    public void testStartMembersRefListBelowUserTree() throws Exception {
        init(true);
        Tree userTree = createUserTree();
        Tree memberRefList = userTree.addChild(REP_MEMBERS_LIST);
        memberRefList.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBER_REFERENCES_LIST);

        assertFalse(importer.start(memberRefList));
    }

    @Test
    public void testStartMembersRefBelowAnyTree() throws Exception {
        init(true);
        Tree memberRefList = root.getTree(PathUtils.ROOT_PATH).addChild(REP_MEMBERS_LIST);
        memberRefList.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBER_REFERENCES_LIST);

        assertFalse(importer.start(memberRefList));
    }

    @Test
    public void testStartRepMembersTree() throws Exception {
        init(true);
        Tree groupTree = createGroupTree();
        Tree repMembers = groupTree.addChild("memberTree");
        repMembers.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBERS);

        repMembers = repMembers.addChild("memberTree");
        repMembers.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBERS);

        assertTrue(importer.start(repMembers));
    }

    @Test
    public void testStartRepMembersBelowUserTree() throws Exception {
        init(true);
        Tree userTree = createUserTree();
        Tree repMembers = userTree.addChild("memberTree");
        repMembers.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBERS);

        repMembers = repMembers.addChild("memberTree");
        repMembers.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBERS);

        assertFalse(importer.start(repMembers));
    }

    @Test
    public void testStartRepMembersBelowAnyTree() throws Exception {
        init(true);
        Tree repMembers = root.getTree(PathUtils.ROOT_PATH).addChild("memberTree");
        repMembers.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBERS);

        repMembers = repMembers.addChild("memberTree");
        repMembers.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBERS);

        assertFalse(importer.start(repMembers));
    }

    //-----------------------------------------------------< startChildInfo >---

    @Test(expected = IllegalStateException.class)
    public void testStartChildInfoIllegalState() throws Exception {
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, "member1")));
    }

    @Test(expected = IllegalStateException.class)
    public void testStartChildInfoWithoutValidStart() throws Exception {
        init(true);
        Tree memberRefList = root.getTree(PathUtils.ROOT_PATH).addChild(REP_MEMBERS_LIST);
        memberRefList.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBER_REFERENCES_LIST);
        importer.start(memberRefList);

        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, "member1")));
    }

    @Test
    public void testStartChildInfoWithoutRepMembersProperty() throws Exception {
        init(true);
        Tree groupTree = createGroupTree();
        Tree memberRefList = groupTree.addChild(REP_MEMBERS_LIST);
        memberRefList.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBER_REFERENCES_LIST);

        importer.start(memberRefList);
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.<PropInfo>of());
    }

    @Test
    public void testStartChildInfoWithRepMembersProperty() throws Exception {
        init(true);
        Tree groupTree = createGroupTree();
        Tree memberRefList = groupTree.addChild(REP_MEMBERS_LIST);
        memberRefList.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBER_REFERENCES_LIST);

        importer.start(memberRefList);
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, "member1")));
    }

    @Test
    public void testStartRepMembersChildInfo() throws Exception {
        init(true);
        Tree groupTree = createGroupTree();

        Tree repMembers = groupTree.addChild("memberTree");
        repMembers.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBERS);
        importer.start(repMembers);
        importer.startChildInfo(createNodeInfo("memberTree", NT_REP_MEMBERS), ImmutableList.<PropInfo>of(createPropInfo("anyProp", "memberValue")));
    }

    @Test
    public void testStartOtherChildInfo() throws Exception {
        init(true);
        Tree groupTree = createGroupTree();
        Tree memberRefList = groupTree.addChild(REP_MEMBERS_LIST);
        memberRefList.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBER_REFERENCES_LIST);

        importer.start(memberRefList);
        importer.startChildInfo(createNodeInfo("memberRef", NodeTypeConstants.NT_OAK_UNSTRUCTURED), ImmutableList.of(createPropInfo(REP_MEMBERS, "member1")));
    }
}