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

import javax.jcr.nodetype.ConstraintViolationException;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.xml.ImportBehavior;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class UserImporterMembershipAbortTest extends UserImporterMembershipIgnoreTest {

    @Override
    String getImportBehavior() {
        return ImportBehavior.NAME_ABORT;
    }

    @Test(expected = ConstraintViolationException.class)
    public void testUnknownMember() throws Exception {
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, unknownContentId)));
        importer.processReferences();
    }

    @Test(expected = ConstraintViolationException.class)
    public void testMixedMembers() throws Exception {
        importer.startChildInfo(createNodeInfo("memberRef", NT_REP_MEMBER_REFERENCES), ImmutableList.of(createPropInfo(REP_MEMBERS, unknownContentId, knownMemberContentId)));
        importer.processReferences();
    }

    @Test(expected = ConstraintViolationException.class)
    public void testAddSameAsMember() throws Exception {
        String contentId = userProvider.getContentID(groupTree);

        // NOTE: reversed over of import compared to 'testNewMembers'
        assertTrue(importer.handlePropInfo(groupTree, createPropInfo(REP_MEMBERS, contentId), mockPropertyDefinition(NT_REP_MEMBER_REFERENCES, true)));
        importer.processReferences();
    }

    @Test(expected = ConstraintViolationException.class)
    public void testNewMembersToEveryone() throws Exception {
        groupTree.setProperty(REP_MEMBERS, ImmutableList.of(knownMemberContentId), Type.STRINGS);
        groupTree.setProperty(REP_PRINCIPAL_NAME, EveryonePrincipal.NAME);

        Tree userTree = createUserTree();
        String contentId = userProvider.getContentID(userTree);

        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_AUTHORIZABLE_ID, TEST_USER_ID), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false)));
        assertTrue(importer.handlePropInfo(groupTree, createPropInfo(REP_MEMBERS, contentId), mockPropertyDefinition(NT_REP_MEMBER_REFERENCES, true)));
        importer.processReferences();
    }
}