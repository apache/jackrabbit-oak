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

import java.util.ArrayList;
import java.util.List;
import javax.jcr.RepositoryException;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class UserImporterMembershipIgnoreTest extends UserImporterBaseTest {

    Tree groupTree;
    Tree memberRefList;

    private UserProvider userProvider;

    String knownMemberContentId;
    String unknownContentId;

    @Override
    public void before() throws Exception {
        super.before();

        userProvider = new UserProvider(root, ConfigurationParameters.EMPTY);
        knownMemberContentId = userProvider.getContentID(testUser.getID());
        unknownContentId = userProvider.getContentID("member1");

        init();
        groupTree = createGroupTree();
        groupTree.setProperty(REP_PRINCIPAL_NAME, "groupPrincipal");
        memberRefList = groupTree.addChild(REP_MEMBERS_LIST);
        memberRefList.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_MEMBER_REFERENCES_LIST);

        assertTrue(importer.start(memberRefList));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidMemberContentId() throws Exception {
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, "memberId")));
        importer.processReferences();
    }

    @Test
    public void testUnknownMember() throws Exception {
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, unknownContentId)));
        importer.processReferences();

        // default importbehavior == IGNORE
        assertFalse(memberRefList.hasChild("memberRef"));
    }

    @Test
    public void testKnownMemberThresholdNotReached() throws Exception {
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, knownMemberContentId)));
        importer.processReferences();

        assertTrue(groupTree.hasProperty(REP_MEMBERS));
    }

    @Test
    public void testKnownMemberThresholdReached() throws Exception {
        List<String> memberIds = new ArrayList();
        for (int i = 0; i <= MembershipWriter.DEFAULT_MEMBERSHIP_THRESHHOLD; i++) {
            memberIds.add(userProvider.getContentID("m"+i));
        }
        groupTree.setProperty(REP_MEMBERS, memberIds, Type.STRINGS);

        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, knownMemberContentId)));
        importer.processReferences();

        assertEquals(1, memberRefList.getChildrenCount(100));
        assertTrue(memberRefList.getChildren().iterator().next().hasProperty(REP_MEMBERS));
    }

    @Test
    public void testMixedMembers() throws Exception {
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, unknownContentId, knownMemberContentId)));
        importer.processReferences();

        assertFalse(memberRefList.hasChild("memberRef"));
    }

    @Test(expected = RepositoryException.class)
    public void testGroupRemovedBeforeProcessing() throws Exception {
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, knownMemberContentId)));

        groupTree.remove();
        importer.processReferences();
    }

    @Test(expected = RepositoryException.class)
    public void testUserConvertedGroupBeforeProcessing() throws Exception {
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, knownMemberContentId)));
        groupTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_USER);
        importer.processReferences();
    }

    @Test
    public void testReplaceExistingProperty() throws Exception {
//        groupTree.setProperty(REP_MEMBERS, ImmutableList.of(unknownContentId), Type.STRINGS);
//
//        assertTrue(importer.handlePropInfo(groupTree, createPropInfo(REP_MEMBERS, testUser.getPrincipal().getName()), mockPropertyDefinition(NT_REP_USER, true)));
//        importer.processReferences();
//
//        PropertyState impersonators = userTree.getProperty(REP_MEMBERS);
//        assertNotNull(impersonators);
//        assertEquals(ImmutableList.of(testUser.getPrincipal().getName()), impersonators.getValue(Type.STRINGS));
    }

    @Test
    public void testNewMembers() throws Exception {
//        Tree folder = root.getTree(getUserConfiguration().getParameters().getConfigValue(PARAM_USER_PATH, DEFAULT_USER_PATH));
//        Tree impersonatorTree = folder.addChild("impersonatorTree");
//        impersonatorTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_USER, Type.NAME);
//        impersonatorTree.setProperty(JcrConstants.JCR_UUID, new UserProvider(root, ConfigurationParameters.EMPTY).getContentID("impersonator1"));
//
//        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, "impersonator1"), mockPropertyDefinition(NT_REP_USER, true)));
//        assertTrue(importer.handlePropInfo(impersonatorTree, createPropInfo(REP_PRINCIPAL_NAME, "impersonator1"), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false)));
//
//        importer.processReferences();
//
//        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
//        assertNotNull(impersonators);
//        assertEquals(ImmutableList.of("impersonator1"), impersonators.getValue(Type.STRINGS));
    }

    @Test
    public void testNewMembers2() throws Exception {
//        Tree folder = root.getTree(getUserConfiguration().getParameters().getConfigValue(PARAM_USER_PATH, DEFAULT_USER_PATH));
//        Tree impersonatorTree = folder.addChild("impersonatorTree");
//        impersonatorTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_USER, Type.NAME);
//        impersonatorTree.setProperty(JcrConstants.JCR_UUID, new UserProvider(root, ConfigurationParameters.EMPTY).getContentID("impersonator1"));
//
//        // NOTE: reversed over of import compared to 'testNewImpersonator'
//        assertTrue(importer.handlePropInfo(impersonatorTree, createPropInfo(REP_PRINCIPAL_NAME, "impersonator1"), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false)));
//        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, "impersonator1"), mockPropertyDefinition(NT_REP_USER, true)));
//
//        importer.processReferences();
//
//        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
//        assertNotNull(impersonators);
//        assertEquals(ImmutableList.of("impersonator1"), impersonators.getValue(Type.STRINGS));
    }
}