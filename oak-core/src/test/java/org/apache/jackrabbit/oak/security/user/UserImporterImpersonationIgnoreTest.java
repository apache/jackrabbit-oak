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

import org.apache.jackrabbit.guava.common.collect.ImmutableList;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.collections.CollectionUtils;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.spi.security.ConfigurationParameters;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.oak.spi.security.user.action.UserAction;
import org.junit.Test;

import javax.jcr.RepositoryException;

import java.security.Principal;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;

public class UserImporterImpersonationIgnoreTest extends UserImporterBaseTest {

    Tree userTree;

    @Override
    public void before() throws Exception {
        super.before();

        init(true, UserAction.class);
        userTree = createUserTree();
    }

    @Test
    public void testUnknownImpersonators() throws Exception {
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, "impersonator1", "impersonator2"), mockPropertyDefinition(NT_REP_USER, true)));
        importer.processReferences();

        // default importbehavior == IGNORE
        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        assertNull(impersonators);
    }

    @Test
    public void testKnownImpersonators() throws Exception {
        userTree.setProperty(UserConstants.REP_PRINCIPAL_NAME, TEST_USER_ID);
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, testUser.getPrincipal().getName()), mockPropertyDefinition(NT_REP_USER, true)));
        importer.processReferences();

        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        assertNotNull(impersonators);
        assertEquals(ImmutableList.of(testUser.getPrincipal().getName()), impersonators.getValue(Type.STRINGS));
    }

    @Test
    public void testMixedImpersonators() throws Exception {
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, "impersonator1", testUser.getPrincipal().getName()), mockPropertyDefinition(NT_REP_USER, true)));
        importer.processReferences();

        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        assertNotNull(impersonators);
        assertEquals(ImmutableList.of(testUser.getPrincipal().getName()), impersonators.getValue(Type.STRINGS));
    }

    @Test(expected = RepositoryException.class)
    public void testUserRemovedBeforeProcessing() throws Exception {
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, testUser.getPrincipal().getName()), mockPropertyDefinition(NT_REP_USER, true)));
        userTree.remove();
        importer.processReferences();
    }

    @Test(expected = RepositoryException.class)
    public void testUserConvertedGroupBeforeProcessing() throws Exception {
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, testUser.getPrincipal().getName()), mockPropertyDefinition(NT_REP_USER, true)));
        userTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_GROUP);
        importer.processReferences();
    }

    @Test
    public void testReplaceExistingProperty() throws Exception {
        userTree.setProperty(REP_IMPERSONATORS, ImmutableList.of("impersonator1"), Type.STRINGS);

        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, testUser.getPrincipal().getName()), mockPropertyDefinition(NT_REP_USER, true)));
        importer.processReferences();

        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        assertNotNull(impersonators);
        assertEquals(ImmutableList.of(testUser.getPrincipal().getName()), impersonators.getValue(Type.STRINGS));
    }

    @Test
    public void testReplaceExistingProperty2() throws Exception {
        userTree.setProperty(REP_IMPERSONATORS, ImmutableList.of("impersonator1"), Type.STRINGS);

        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, "impersonator1", testUser.getPrincipal().getName()), mockPropertyDefinition(NT_REP_USER, true)));
        importer.processReferences();

        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        assertNotNull(impersonators);
        assertEquals(Set.of("impersonator1", testUser.getPrincipal().getName()), CollectionUtils.toSet(impersonators.getValue(Type.STRINGS)));
    }

    @Test
    public void testNewImpersonator() throws Exception {
        Tree folder = root.getTree(getUserConfiguration().getParameters().getConfigValue(PARAM_USER_PATH, DEFAULT_USER_PATH));
        Tree impersonatorTree = folder.addChild("impersonatorTree");
        impersonatorTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_USER, Type.NAME);
        impersonatorTree.setProperty(JcrConstants.JCR_UUID, new UserProvider(root, ConfigurationParameters.EMPTY).getContentID("impersonator1"));

        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, "impersonator1"), mockPropertyDefinition(NT_REP_USER, true)));
        assertTrue(importer.handlePropInfo(impersonatorTree, createPropInfo(REP_PRINCIPAL_NAME, "impersonator1"), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false)));

        importer.processReferences();

        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        assertNotNull(impersonators);
        assertEquals(ImmutableList.of("impersonator1"), impersonators.getValue(Type.STRINGS));
    }

    @Test
    public void testNewImpersonator2() throws Exception {
        Tree folder = root.getTree(getUserConfiguration().getParameters().getConfigValue(PARAM_USER_PATH, DEFAULT_USER_PATH));
        Tree impersonatorTree = folder.addChild("impersonatorTree");
        impersonatorTree.setProperty(JcrConstants.JCR_PRIMARYTYPE, NT_REP_USER, Type.NAME);
        impersonatorTree.setProperty(JcrConstants.JCR_UUID, new UserProvider(root, ConfigurationParameters.EMPTY).getContentID("impersonator1"));

        // NOTE: reversed over of import compared to 'testNewImpersonator'
        assertTrue(importer.handlePropInfo(impersonatorTree, createPropInfo(REP_PRINCIPAL_NAME, "impersonator1"), mockPropertyDefinition(NT_REP_AUTHORIZABLE, false)));
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, "impersonator1"), mockPropertyDefinition(NT_REP_USER, true)));

        importer.processReferences();

        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        assertNotNull(impersonators);
        assertEquals(ImmutableList.of("impersonator1"), impersonators.getValue(Type.STRINGS));
    }

    @Test
    public void testGrantImpersonationGroupPrincipal() throws Exception {
        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS, EveryonePrincipal.NAME), mockPropertyDefinition(NT_REP_USER, true)));
        importer.processReferences();

        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        assertNull(impersonators);
    }

    @Test
    public void testRevokeImpersonationAlreadyRemoved() throws Exception {
        UserAction ua = (UserAction) testAction;
        doAnswer(invocationOnMock -> {
            userTree.removeProperty(REP_IMPERSONATORS);
            return null;
        }).when(ua).onRevokeImpersonation(any(User.class), any(Principal.class), any(Root.class), any(NamePathMapper.class));

        userTree.setProperty(REP_IMPERSONATORS, ImmutableList.of("impersonator1"), Type.STRINGS);

        assertTrue(importer.handlePropInfo(userTree, createPropInfo(REP_IMPERSONATORS), mockPropertyDefinition(NT_REP_USER, true)));
        importer.processReferences();

        PropertyState impersonators = userTree.getProperty(REP_IMPERSONATORS);
        assertNull(impersonators);
    }
}