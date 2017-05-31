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
package org.apache.jackrabbit.oak.security.user.query;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;
import javax.jcr.ValueFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.AbstractSecurityTest;
import org.apache.jackrabbit.oak.plugins.value.jcr.ValueFactoryImpl;
import org.apache.jackrabbit.oak.security.user.UserManagerImpl;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.user.AuthorizableType;
import org.junit.Before;
import org.junit.Test;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * UserQueryManagerTest provides test cases for {@link UserQueryManager}.
 * This class include the original jr2.x test-cases provided by
 * {@code NodeResolverTest} and {@code IndexNodeResolverTest}.
 */
public class UserQueryManagerTest extends AbstractSecurityTest {

    private ValueFactory valueFactory;
    private UserQueryManager queryMgr;
    private User user;
    private String userId;
    private String propertyName;

    private List<Group> groups = new ArrayList();

    @Before
    public void before() throws Exception {
        super.before();

        UserManagerImpl userMgr = (UserManagerImpl) getUserManager(root);
        user = getTestUser();
        userId = user.getID();
        queryMgr = new UserQueryManager(userMgr, namePathMapper, getUserConfiguration().getParameters(), root);

        valueFactory = new ValueFactoryImpl(root, namePathMapper);
        propertyName = "testProperty";

        getQueryEngineSettings().setFailTraversal(false);
    }

    @Override
    public void after() throws Exception {
        try {
            getQueryEngineSettings().setFailTraversal(true);
            for (Group g : groups) {
                g.remove();
            }
            if (root.hasPendingChanges()) {
                root.commit();
            }
        } finally {
            super.after();
        }
    }

    private Group createGroup(@Nullable String id, @Nullable Principal principal) throws RepositoryException {
        Group g;
        if (id != null) {
            if (principal != null) {
                g = getUserManager(root).createGroup(id, principal, null);
            } else {
                g = getUserManager(root).createGroup(id);
            }
        } else {
            checkNotNull(principal);
            g = getUserManager(root).createGroup(principal);
        }
        groups.add(g);
        return g;
    }

    private static void assertResultContainsAuthorizables(@Nonnull Iterator<Authorizable> result, Authorizable... expected) throws RepositoryException {
        switch (expected.length) {
            case 0:
                assertFalse(result.hasNext());
                break;
            case 1:
                assertTrue(result.hasNext());
                assertEquals(expected[0].getID(), result.next().getID());
                assertFalse(result.hasNext());
                break;
            default:
                assertEquals(ImmutableSet.copyOf(expected), ImmutableSet.copyOf(result));
        }
    }

    /**
     * @since OAK 1.0
     */
    @Test
    public void testFindNodesExact() throws Exception {
        Value vs = valueFactory.createValue("value \\, containing backslash");
        user.setProperty(propertyName, vs);
        root.commit();

        try {
            Iterator<Authorizable> result = queryMgr.findAuthorizables(propertyName, "value \\, containing backslash", AuthorizableType.USER, true);
            assertTrue("expected result", result.hasNext());
            assertEquals(user.getID(), result.next().getID());
            assertFalse("expected no more results", result.hasNext());
        } finally {
            user.removeProperty(propertyName);
            root.commit();
        }
    }

    @Test
    public void testFindNodesNonExact() throws Exception {
        Value vs = valueFactory.createValue("value \\, containing backslash");
        user.setProperty(propertyName, vs);
        root.commit();

        try {
            Iterator<Authorizable> result = queryMgr.findAuthorizables(propertyName, "value \\, containing backslash", AuthorizableType.USER, false);
            assertTrue("expected result", result.hasNext());
            assertEquals(user.getID(), result.next().getID());
            assertFalse("expected no more results", result.hasNext());
        } finally {
            user.removeProperty(propertyName);
            root.commit();
        }
    }

    @Test
    public void testFindNodesNonExactWithApostrophe() throws Exception {
        Value vs = valueFactory.createValue("value ' with apostrophe");
        try {
            user.setProperty(propertyName, vs);
            root.commit();

            Iterator<Authorizable> result = queryMgr.findAuthorizables(propertyName, "value ' with apostrophe", AuthorizableType.USER, false);

            assertTrue("expected result", result.hasNext());
            assertEquals(user.getID(), result.next().getID());
            assertFalse("expected no more results", result.hasNext());
        } finally {
            user.removeProperty(propertyName);
            root.commit();
        }
    }

    @Test
    public void testFindNodesExactWithApostrophe() throws Exception {
        Value vs = valueFactory.createValue("value ' with apostrophe");
        try {
            user.setProperty(propertyName, vs);
            root.commit();

            Iterator<Authorizable> result = queryMgr.findAuthorizables(propertyName, "value ' with apostrophe", AuthorizableType.USER, true);
            assertTrue("expected result", result.hasNext());
            assertEquals(user.getID(), result.next().getID());
            assertFalse("expected no more results", result.hasNext());
        } finally {
            user.removeProperty(propertyName);
            root.commit();
        }
    }

    @Test
    public void testQueryMaxCountZero() throws Exception {
        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> queryBuilder) {
                queryBuilder.setLimit(0, 0);

            }
        };
        assertSame(Iterators.emptyIterator(), queryMgr.findAuthorizables(q));
    }

    @Test
    public void testQueryScopeEveryoneNonExisting() throws Exception {
        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.nameMatches(userId));
                builder.setScope(EveryonePrincipal.NAME, false);
            }
        };

        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertResultContainsAuthorizables(result, user);
    }

    @Test
    public void testQueryScopeEveryoneFiltersEveryone() throws Exception {
        Value v = getValueFactory(root).createValue("value");

        Group g = createGroup(null, EveryonePrincipal.getInstance());
        g.setProperty(propertyName, v);
        user.setProperty(propertyName, v);
        root.commit();

        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.eq(propertyName, v));
                builder.setScope(EveryonePrincipal.NAME, false);
            }
        };

        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertResultContainsAuthorizables(result, user);
    }

    @Test
    public void testQueryScopeEveryoneWithIdDiffersPrincipalName() throws Exception {
        Value v = getValueFactory(root).createValue("value");

        Group g = createGroup("eGroup", EveryonePrincipal.getInstance());
        g.setProperty(propertyName, v);
        user.setProperty(propertyName, v);
        root.commit();

        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.eq(propertyName, v));
                builder.setScope("eGroup", false);
            }
        };

        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertResultContainsAuthorizables(result, user);
    }

    @Test
    public void testQueryNoScope() throws Exception {
        Value v = getValueFactory(root).createValue("value");

        Group g = createGroup(null, EveryonePrincipal.getInstance());
        g.setProperty(propertyName, v);
        user.setProperty(propertyName, v);
        root.commit();

        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.eq(propertyName, v));
            }
        };

        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertResultContainsAuthorizables(result, user, g);
    }

    @Test
    public void testQueryScopeNotMember() throws Exception {
        Value v = getValueFactory(root).createValue("value");

        Group g = createGroup("g1", null);
        user.setProperty(propertyName, v);
        root.commit();

        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.eq(propertyName, v));
                builder.setScope("g1", false);
            }
        };

        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertResultContainsAuthorizables(result);
    }

    @Test
    public void testQueryScopeDeclaredMember() throws Exception {
        Value v = getValueFactory(root).createValue("value");

        Group g = createGroup("g1", null);
        g.addMember(user);
        user.setProperty(propertyName, v);
        root.commit();

        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.eq(propertyName, v));
                builder.setScope("g1", false);
            }
        };

        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertResultContainsAuthorizables(result, user);
    }

    @Test
    public void testQueryScopeDeclaredMembership() throws Exception {
        Value v = getValueFactory(root).createValue("value");

        Group g = createGroup("g1", null);
        Group g2 = createGroup("g2", null);
        g.addMember(g2);
        g2.addMember(user);
        user.setProperty(propertyName, v);
        root.commit();

        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.eq(propertyName, v));
                builder.setScope("g1", true);
            }
        };

        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertResultContainsAuthorizables(result);
    }

    @Test
    public void testQueryScopeInheritedMembership() throws Exception {
        Value v = getValueFactory(root).createValue("value");

        Group g = createGroup("g1", null);
        Group g2 = createGroup("g2", null);
        g.addMember(g2);
        g2.addMember(user);
        user.setProperty(propertyName, v);
        root.commit();

        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.eq(propertyName, v));
                builder.setScope("g1", false);
            }
        };
        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertResultContainsAuthorizables(result, user);
    }

    @Test
    public void testQueryBoundWithoutSortOrder() throws Exception {
        ValueFactory vf = getValueFactory(root);
        Group g = createGroup("g1", null);
        g.setProperty(propertyName, vf.createValue(50));
        Group g2 = createGroup("g2", null);
        g2.setProperty(propertyName, vf.createValue(60));
        user.setProperty(propertyName, vf.createValue(101));
        root.commit();

        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setLimit(vf.createValue(100), Long.MAX_VALUE);
                builder.setCondition(builder.gt(propertyName, vf.createValue(20)));
            }
        };

        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertResultContainsAuthorizables(result, user, g, g2);
    }

    @Test
    public void testQueryBoundWithSortOrder() throws Exception {
        ValueFactory vf = getValueFactory(root);
        Group g = createGroup("g1", null);
        g.setProperty(propertyName, vf.createValue(50));
        Group g2 = createGroup("g2", null);
        g2.setProperty(propertyName, vf.createValue(60));
        user.setProperty(propertyName, vf.createValue(101));
        root.commit();

        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setLimit(vf.createValue(100), Long.MAX_VALUE);
                builder.setSortOrder(propertyName, QueryBuilder.Direction.ASCENDING);
                builder.setCondition(builder.gt(propertyName, vf.createValue(20)));
            }
        };

        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertResultContainsAuthorizables(result, user);
    }

    @Test
    public void testQueryBoundWithSortOrderMissingCondition() throws Exception {
        ValueFactory vf = getValueFactory(root);
        Group g = createGroup("g1", null);
        g.setProperty(propertyName, vf.createValue(50));
        Group g2 = createGroup("g2", null);
        g2.setProperty(propertyName, vf.createValue(60));
        user.setProperty(propertyName, vf.createValue(101));
        root.commit();

        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setLimit(vf.createValue(100), Long.MAX_VALUE);
                builder.setSortOrder(propertyName, QueryBuilder.Direction.ASCENDING);
            }
        };

        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertResultContainsAuthorizables(result, user);
    }

    @Test
    public void testQuerySortIgnoreCase() throws Exception {
        ValueFactory vf = getValueFactory(root);
        Group g = createGroup("g1", null);
        g.setProperty(propertyName, vf.createValue("aaa"));
        Group g2 = createGroup("g2", null);
        g2.setProperty(propertyName, vf.createValue("BBB"));
        user.setProperty(propertyName, vf.createValue("c"));
        root.commit();

        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.exists(propertyName));
                builder.setSortOrder(propertyName, QueryBuilder.Direction.DESCENDING, true);
            }
        };

        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertEquals(ImmutableList.of(user, g2, g), ImmutableList.copyOf(result));
    }

    @Test
    public void testQuerySortRespectCase() throws Exception {
        ValueFactory vf = getValueFactory(root);
        Group g = createGroup("g1", null);
        g.setProperty(propertyName, vf.createValue("aaa"));
        Group g2 = createGroup("g2", null);
        g2.setProperty(propertyName, vf.createValue("BBB"));
        user.setProperty(propertyName, vf.createValue("c"));
        root.commit();

        Query q = new Query() {
            @Override
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.exists(propertyName));
                builder.setSortOrder(propertyName, QueryBuilder.Direction.DESCENDING, false);
            }
        };

        Iterator<Authorizable> result = queryMgr.findAuthorizables(q);
        assertEquals(ImmutableList.of(user, g, g2), ImmutableList.copyOf(result));
    }
}