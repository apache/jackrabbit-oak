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

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import javax.annotation.Nonnull;
import javax.jcr.NoSuchWorkspaceException;
import javax.jcr.RepositoryException;
import javax.security.auth.Subject;
import javax.security.auth.login.LoginException;

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
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.security.authentication.SystemSubject;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.util.NodeUtil;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
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
            if (testGroup != null) {
                testGroup.remove();
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    private Tree getAuthorizableTree(@Nonnull Authorizable authorizable) throws RepositoryException {
        return root.getTree(authorizable.getPath());
    }

    private Tree getCache(@Nonnull Authorizable authorizable) throws Exception {
        ContentSession cs = Subject.doAs(SystemSubject.INSTANCE, new PrivilegedExceptionAction<ContentSession>() {
            @Override
            public ContentSession run() throws LoginException, NoSuchWorkspaceException {
                return login(null);

            }
        });
        try {
            Root r = cs.getLatestRoot();
            NodeUtil n = new NodeUtil(r.getTree(authorizable.getPath()));
            NodeUtil c = n.getOrAddChild(CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE);
            c.setLong(CacheConstants.REP_EXPIRATION, 1);
            r.commit(CacheValidatorProvider.asCommitAttributes());
        } finally {
            cs.close();
        }

        root.refresh();
        return root.getTree(authorizable.getPath()).getChild(CacheConstants.REP_CACHE);
    }

    @Test
    public void testCreateCacheByName() throws RepositoryException {
        for (Authorizable a : authorizables) {
            try {
                NodeUtil node = new NodeUtil(getAuthorizableTree(a));
                node.addChild(CacheConstants.REP_CACHE, JcrConstants.NT_UNSTRUCTURED);
                root.commit();
                fail("Creating rep:cache node below a user or group must fail.");
            } catch (CommitFailedException e) {
                assertTrue(e.isConstraintViolation());
                assertEquals(34, e.getCode());
            } finally {
                root.refresh();
            }
        }
    }

    @Test
    public void testCreateCacheByNodeType() throws RepositoryException {
        for (Authorizable a : authorizables) {
            try {
                NodeUtil node = new NodeUtil(getAuthorizableTree(a));
                NodeUtil cache = node.addChild("childNode", CacheConstants.NT_REP_CACHE);
                cache.setLong(CacheConstants.REP_EXPIRATION, 1);
                root.commit();
                fail("Creating node with nt rep:Cache below a user or group must fail.");
            } catch (CommitFailedException e) {
                assertTrue(e.isConstraintViolation());
                assertEquals(34, e.getCode());
            } finally {
                root.refresh();
            }
        }
    }

    @Test
    public void testChangePrimaryType() throws RepositoryException {
        for (Authorizable a : authorizables) {
            try {
                NodeUtil node = new NodeUtil(getAuthorizableTree(a));
                NodeUtil cache = node.addChild("childNode", JcrConstants.NT_UNSTRUCTURED);
                root.commit();

                cache.setName(JcrConstants.JCR_PRIMARYTYPE, CacheConstants.NT_REP_CACHE);
                cache.setLong(CacheConstants.REP_EXPIRATION, 1);
                root.commit();
                fail("Changing primary type of residual node below an user/group to rep:Cache must fail.");
            } catch (CommitFailedException e) {
                assertTrue(e.isConstraintViolation());
                assertEquals(34, e.getCode());
            } finally {
                root.refresh();
            }
        }
    }

    @Test
    public void testCreateCacheWithCommitInfo() throws RepositoryException {
        for (Authorizable a : authorizables) {
            try {
                NodeUtil node = new NodeUtil(getAuthorizableTree(a));
                NodeUtil cache = node.addChild(CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE);
                cache.setLong(CacheConstants.REP_EXPIRATION, 1);
                root.commit(CacheValidatorProvider.asCommitAttributes());
                fail("Creating rep:cache node below a user or group must fail.");
            } catch (CommitFailedException e) {
                assertTrue(e.isConstraintViolation());
                assertEquals(34, e.getCode());
            } finally {
                root.refresh();
            }
        }
    }

    @Test
    public void testCreateCacheBelowProfile() throws Exception {
        try {
            NodeUtil node = new NodeUtil(getAuthorizableTree(getTestUser()));
            NodeUtil child = node.addChild("profile", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            child.addChild(CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE).setLong(CacheConstants.REP_EXPIRATION, 23);
            root.commit(CacheValidatorProvider.asCommitAttributes());
            fail("Creating rep:cache node below a user or group must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(34, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testCreateCacheBelowPersistedProfile() throws Exception {
        try {
            NodeUtil node = new NodeUtil(getAuthorizableTree(getTestUser()));
            NodeUtil child = node.addChild("profile", NodeTypeConstants.NT_OAK_UNSTRUCTURED);
            root.commit();

            child.addChild(CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE).setLong(CacheConstants.REP_EXPIRATION, 23);
            root.commit(CacheValidatorProvider.asCommitAttributes());
            fail("Creating rep:cache node below a user or group must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(34, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testModifyCache() throws Exception {
        List<PropertyState> props = new ArrayList();
        props.add(PropertyStates.createProperty(CacheConstants.REP_EXPIRATION, 25));
        props.add(PropertyStates.createProperty(CacheConstants.REP_GROUP_PRINCIPAL_NAMES, EveryonePrincipal.NAME));
        props.add(PropertyStates.createProperty(JcrConstants.JCR_PRIMARYTYPE, JcrConstants.NT_UNSTRUCTURED, Type.NAME));
        props.add(PropertyStates.createProperty("residualProp", "anyvalue"));

        Tree cache = getCache(getTestUser());
        for (PropertyState prop : props) {
            try {
                cache.setProperty(prop);
                root.commit(CacheValidatorProvider.asCommitAttributes());

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
        NodeUtil cache = new NodeUtil(getCache(getTestUser()));
        try {
            NodeUtil c = cache.getOrAddChild(CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE);
            c.setLong(CacheConstants.REP_EXPIRATION, 223);
            root.commit(CacheValidatorProvider.asCommitAttributes());

            fail("Creating nested cache must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(34, e.getCode());
        } finally {
            root.refresh();
        }
    }

    @Test
    public void testRemoveCache() throws Exception {
        Tree cache = getCache(getTestUser());
        cache.remove();
        root.commit();
    }

    @Test
    public void testCreateCacheOutsideOfAuthorizable() throws Exception {
        NodeUtil n = new NodeUtil(root.getTree("/"));
        try {
            NodeUtil child = n.addChild(CacheConstants.REP_CACHE, CacheConstants.NT_REP_CACHE);
            child.setLong(CacheConstants.REP_EXPIRATION, 1);
            root.commit();
            fail("Using rep:cache/rep:Cache outside a user or group must fail.");
        } catch (CommitFailedException e) {
            assertTrue(e.isConstraintViolation());
            assertEquals(34, e.getCode());
        } finally {
            root.refresh();
            Tree c = n.getTree().getChild(CacheConstants.REP_CACHE);
            if (c.exists()) {
                c.remove();
                root.commit();
            }
        }

    }
}