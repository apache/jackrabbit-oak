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

import java.util.Collections;
import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.security.user.cache.CachedMembershipReader;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.cache.CacheConstants;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import javax.jcr.RepositoryException;
import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class CacheValidatorProviderTest extends AbstractSecurityTest {

    private Group testGroup;
    private Authorizable[] authorizables;

    @Override
    public void before() throws Exception {
        super.before();

        testGroup = getUserManager(root).createGroup("testGroup_" + UUID.randomUUID());
        root.commit();

        authorizables = new Authorizable[] {getTestUser(), testGroup};
    }

    @Override
    public void after() throws Exception {
        try {
            root.refresh();
            if (testGroup != null) {
                testGroup.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    private Tree getAuthorizableTree(@NotNull Authorizable authorizable) throws RepositoryException {
        return root.getTree(authorizable.getPath());
    }

    private Tree getCache(@NotNull Authorizable authorizable) throws Exception {
        // Creating CachedMembershipReader as this is the only class allowed to write in rep:cache
        try (ContentSession cs = Subject.doAs(SystemSubject.INSTANCE, (PrivilegedExceptionAction<ContentSession>) () -> login(null))) {
            Root r = cs.getLatestRoot();
            Tree n = r.getTree(authorizable.getPath());
            CachedMembershipReader reader = new CachedPrincipalMembershipReader(
                    CacheConfiguration.fromUserConfiguration(getUserConfiguration(), UserPrincipalProvider.REP_GROUP_PRINCIPAL_NAMES),
                    r,
                    PrincipalImpl::new);
            reader.readMembership(n, tree -> Collections.singleton(new PrincipalImpl(tree.getName())));
        }

        root.refresh();
        return root.getTree(authorizable.getPath()).getChild(CacheConstants.REP_CACHE);
    }

    @Test
    public void testCreateCacheByName() throws RepositoryException {
        for (Authorizable a : authorizables) {
            try {
                Tree node = getAuthorizableTree(a);
                TreeUtil.addChild(node, CacheConstants.REP_CACHE, JcrConstants.NT_UNSTRUCTURED);
                root.commit();
                fail("Creating rep:cache node below a user or group must fail.");
            } catch (CommitFailedException e) {
                assertTrue(e.isConstraintViolation());
                assertEquals(34, e.getCode());
            }
        }
    }

    @Test
    public void testCreateCacheByNodeType() throws RepositoryException {
        for (Authorizable a : authorizables) {
            try {
                Tree node = getAuthorizableTree(a);
                Tree cache = TreeUtil.addChild(node, "childNode", CacheConstants.NT_REP_CACHE);
                cache.setProperty(CacheConstants.REP_EXPIRATION, 1L, Type.LONG);
                root.commit();
                fail("Creating node with nt rep:Cache below a user or group must fail.");
            } catch (CommitFailedException e) {
                assertTrue(e.isConstraintViolation());
                assertEquals(34, e.getCode());
            }
        }
    }

    @Test
    public void testChangePrimaryTypeUser() throws Exception {
        for (Authorizable a : authorizables) {
            try {
                Tree node = getAuthorizableTree(a);
                Tree cache = TreeUtil.addChild(node, "childNode", JcrConstants.NT_UNSTRUCTURED);
                root.commit();

                cache.setProperty(JCR_PRIMARYTYPE, CacheConstants.NT_REP_CACHE, Type.NAME);
                cache.setProperty(CacheConstants.REP_EXPIRATION, 1L, Type.LONG);
                root.commit();
                fail("Changing primary type of residual node below an user/group to rep:Cache must fail.");
            } catch (CommitFailedException e) {
                assertTrue(e.isConstraintViolation());
                assertEquals(34, e.getCode());
            }
        }
    }

    @Test
    public void testCreateCacheWithCommitInfo() throws RepositoryException {
        for (Authorizable a : authorizables) {
            try {
                Tree node = getAuthorizableTree(a);
                Tree cache = TreeUtil.addChild(node, CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE);
                cache.setProperty(CacheConstants.REP_EXPIRATION, 1L, Type.LONG);
                root.commit();
                fail("Creating rep:cache node below a user or group must fail.");
            } catch (CommitFailedException e) {
                assertTrue(e.isConstraintViolation());
                assertEquals(34, e.getCode());
            }
        }
    }

    @Test
    public void testCreateCacheBelowProfile() throws Exception {
        try {
            Tree node = getAuthorizableTree(getTestUser());
            Tree child = TreeUtil.addChild(node, "profile", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            TreeUtil.addChild(child, CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE).setProperty(
                    CacheConstants.REP_EXPIRATION, 23L, Type.LONG);
            root.commit();
            fail("Creating rep:cache node below a user or group must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(34, e.getCode());
        }
    }

    @Test
    public void testCreateCacheBelowPersistedProfile() throws Exception {
        Tree node = getAuthorizableTree(getTestUser());
        Tree child = TreeUtil.addChild(node, "profile", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        root.commit();

        try {
            TreeUtil.addChild(child, CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE).setProperty(
                    CacheConstants.REP_EXPIRATION, 23L, Type.LONG);
            root.commit();
            fail("Creating rep:cache node below a user or group must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(34, e.getCode());
        }
    }

    @Test
    public void testModifyCache() throws Exception {
        List<PropertyState> props = new ArrayList<>();
        props.add(PropertyStates.createProperty(CacheConstants.REP_EXPIRATION, 25));
        props.add(PropertyStates.createProperty(UserPrincipalProvider.REP_GROUP_PRINCIPAL_NAMES, EveryonePrincipal.NAME));
        props.add(PropertyStates.createProperty(JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME));
        props.add(PropertyStates.createProperty("residualProp", "anyvalue"));

        Tree cache = getCache(getTestUser());
        for (PropertyState prop : props) {
            try {
                cache.setProperty(prop);
                root.commit();

                fail("Modifying rep:cache node below a user or group must fail.");
            } catch (CommitFailedException e) {
                assertTrue(e.isConstraintViolation());
                assertEquals(34, e.getCode());
            } finally {
                root.refresh();
            }
        }
    }

    @Test
    public void testNestedCache() throws Exception {
        Tree cache = getCache(getTestUser());
        try {
            Tree c = TreeUtil.getOrAddChild(cache, CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE);
            c.setProperty(CacheConstants.REP_EXPIRATION, 223L, Type.LONG);
            root.commit();

            fail("Creating nested cache must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(34, e.getCode());
        }
    }

    @Test
    public void testRemoveCache() throws Exception {
        Tree cache = getCache(getTestUser());
        cache.remove();
        root.commit();
        assertFalse(getAuthorizableTree(getTestUser()).hasChild(CacheConstants.REP_CACHE));
    }

    @Test
    public void testCreateCacheOutsideOfAuthorizable() throws Exception {
        Tree n = root.getTree(PathUtils.ROOT_PATH);
        try {
            Tree child = TreeUtil.addChild(n, CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE);
            child.setProperty(CacheConstants.REP_EXPIRATION, 1L, Type.LONG);
            root.commit();
            fail("Using rep:cache/rep:Cache outside a user or group must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(34, e.getCode());
        } finally {
            root.refresh();
            Tree c = n.getChild(CacheConstants.REP_CACHE);
            if (c.exists()) {
                c.remove();
                root.commit();
            }
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testChangeAuthorizableChildToCache() throws Exception {
        Tree authorizableTree = getAuthorizableTree(getTestUser());
        TreeUtil.addChild(authorizableTree, "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
        root.commit();

        try {
            Tree child = getAuthorizableTree(getTestUser()).getChild("child");
            child.setProperty(JCR_PRIMARYTYPE, CacheConstants.NT_REP_CACHE, Type.NAME);
            root.commit();
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(34, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testChildNodeAddedToCache() throws Exception {
        try {
            Tree cache = getCache(getTestUser());
            Tree child = TreeUtil.addChild(cache, "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);

            Validator validator = createCacheValidator(cache);
            validator.childNodeAdded("child", getTreeProvider().asNodeState(child));
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(34, e.getCode());
            throw e;
        }
    }

    @Test(expected = CommitFailedException.class)
    public void testChildNodeChangedToCache() throws Exception {
        try {
            Tree cache = getCache(getTestUser());
            Tree child = TreeUtil.addChild(cache, "child", NodeTypeConstants.NT_OAK_UNSTRUCTURED);

            Validator validator = createCacheValidator(cache);
            NodeState ns = getTreeProvider().asNodeState(child);
            validator.childNodeChanged("child", ns, ns);
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(34, e.getCode());
            throw e;
        }
    }

    @Test
    public void testPropertyChangedToCache() throws Exception {
        try {
            Tree cache = getCache(getTestUser());
            PropertyState prop = PropertyStates.createProperty("aProperty", CacheConstants.NT_REP_CACHE, Type.STRING);

            Validator validator = createCacheValidator(cache);
            validator.propertyChanged(PropertyStates.createProperty("aProperty", NodeTypeConstants.NT_OAK_UNSTRUCTURED, Type.STRING), prop);
            fail("Changing primary type of residual node below an user/group to rep:Cache must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(34, e.getCode());
        }
    }

    @Test
    public void testChangePropertyNotInCache() throws Exception {
        try {
            Tree cache = root.getTree("/random/path");
            PropertyState prop = PropertyStates.createProperty("aProperty", CacheConstants.NT_REP_CACHE, Type.STRING);

            Validator validator = createCacheValidator(cache);
            validator.propertyChanged(PropertyStates.createProperty("aProperty", NodeTypeConstants.NT_OAK_UNSTRUCTURED, Type.STRING), prop);
        } catch (Exception e) {
            fail("Changing property not in cache must not fail.");
        }
    }

    private Validator createCacheValidator(@NotNull Tree rootTree) {
        CacheValidatorProvider provider = new CacheValidatorProvider(root.getContentSession().getAuthInfo().getPrincipals(), getTreeProvider());
        NodeState nodeState = getTreeProvider().asNodeState(rootTree);
        return provider.getRootValidator(nodeState, nodeState, new CommitInfo("sid", "uid"));
    }
}
