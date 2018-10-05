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
package org.apache.jackrabbit.oak.jcr.security.user;

import java.security.Principal;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Query;
import org.apache.jackrabbit.api.security.user.QueryBuilder;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.commons.jackrabbit.user.AuthorizableQueryManager;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;
import org.apache.jackrabbit.oak.spi.security.user.UserConstants;
import org.apache.jackrabbit.util.Text;
import org.junit.Test;

/**
 * Tests for the {@link Query} interface and
 * {@link org.apache.jackrabbit.api.security.user.UserManager#findAuthorizables(Query)}.
 */
public class UserQueryTest extends AbstractUserTest {

    private User kangaroo;
    private User elephant;
    private User goldenToad;

    private final Set<User> users = new HashSet<User>();

    private Group vertebrates;
    private Group mammals;
    private Group apes;

    private final Set<Group> groups = new HashSet<Group>();
    private final Set<Authorizable> authorizables = new HashSet<Authorizable>();

    private final Set<Authorizable> systemDefined = new HashSet<Authorizable>();

    @Override
    public void setUp() throws Exception {
        super.setUp();

        Iterator<Authorizable> systemAuthorizables = userMgr.findAuthorizables("rep:principalName", null);
        while (systemAuthorizables.hasNext()) {
            Authorizable authorizable = systemAuthorizables.next();
            if (authorizable.isGroup()) {
                groups.add((Group) authorizable);
            } else {
                users.add((User) authorizable);
            }
            systemDefined.add(authorizable);
        }

        Group animals = createGroup("animals");
        Group invertebrates = createGroup("invertebrates");
        Group arachnids = createGroup("arachnids");
        Group insects = createGroup("insects");
        vertebrates = createGroup("vertebrates");
        mammals = createGroup("mammals");
        apes = createGroup("apes");
        Group reptiles = createGroup("reptiles");
        Group birds = createGroup("birds");
        Group amphibians = createGroup("amphibians");

        animals.addMember(invertebrates);
        animals.addMember(vertebrates);
        invertebrates.addMember(arachnids);
        invertebrates.addMember(insects);
        vertebrates.addMember(mammals);
        vertebrates.addMember(reptiles);
        vertebrates.addMember(birds);
        vertebrates.addMember(amphibians);
        mammals.addMember(apes);

        User blackWidow = createUser("black widow", "flies", 2, false);
        User gardenSpider = createUser("garden spider", "flies", 2, false);
        User jumpingSpider = createUser("jumping spider", "insects", 1, false);
        addMembers(arachnids, blackWidow, gardenSpider, jumpingSpider);

        User ant = createUser("ant", "leaves", 0.5, false);
        User bee = createUser("bee", "honey", 2.5, true);
        User fly = createUser("fly", "dirt", 1.3, false);
        addMembers(insects, ant, bee, fly);

        User jackrabbit = createUser("jackrabbit", "carrots", 2500, true);
        User backslash = createUser("foo\\bar", "characters", 2500, false);
        User deer = createUser("deer", "leaves", 120000, true);
        User opossum = createUser("opossum", "fruit", 1200, true);
        kangaroo = createUser("kangaroo", "grass", 90000, true);
        elephant = createUser("elephant", "leaves", 5000000, true);
        addMembers(mammals, jackrabbit, deer, opossum, kangaroo, elephant);

        User lemur = createUser("lemur", "nectar", 1100, true);
        User gibbon = createUser("gibbon", "meat", 20000, true);
        addMembers(apes, lemur, gibbon);

        User crocodile = createUser("crocodile", "meat", 80000, false);
        User turtle = createUser("turtle", "leaves", 10000, true);
        User lizard = createUser("lizard", "leaves", 1900, false);
        addMembers(reptiles, crocodile, turtle, lizard);

        User kestrel = createUser("kestrel", "mice", 2000, false);
        User goose = createUser("goose", "snails", 13000, true);
        User pelican = createUser("pelican", "fish", 15000, true);
        User dove = createUser("dove", "insects", 1600, false);
        addMembers(birds, kestrel, goose, pelican, dove);

        User salamander = createUser("salamander", "insects", 800, true);
        goldenToad = createUser("golden toad", "insects", 700, false);
        User poisonDartFrog = createUser("poison dart frog", "insects", 40, false);
        addMembers(amphibians, salamander, goldenToad, poisonDartFrog);

        setProperty("canFly", vf.createValue(true), bee, fly, kestrel, goose, pelican, dove);
        setProperty("poisonous", vf.createValue(true), blackWidow, bee, poisonDartFrog);
        setProperty("poisonous", vf.createValue(false), turtle, lemur);
        setProperty("hasWings", vf.createValue(false), blackWidow, gardenSpider, jumpingSpider, ant,
                jackrabbit, deer, opossum, kangaroo, elephant, lemur, gibbon, crocodile, turtle, lizard,
                salamander, goldenToad, poisonDartFrog);
        setProperty("color", vf.createValue("black"), blackWidow, gardenSpider, ant, fly, lizard, salamander);
        setProperty("color", vf.createValue("WHITE"), opossum, goose, pelican, dove);
        setProperty("color", vf.createValue("gold"), goldenToad);
        setProperty("numberOfLegs", vf.createValue(2), kangaroo, gibbon, kestrel, goose, dove);
        setProperty("numberOfLegs", vf.createValue(4), jackrabbit, deer, opossum, elephant, lemur, crocodile,
                turtle, lizard, salamander, goldenToad, poisonDartFrog);
        setProperty("numberOfLegs", vf.createValue(6), ant, bee, fly);
        setProperty("numberOfLegs", vf.createValue(8), blackWidow, gardenSpider, jumpingSpider);

        // testing ignore-case with sort order
        setProperty("continent", vf.createValue("africa"), lemur, gibbon);
        setProperty("continent", vf.createValue("Africa"), elephant);
        setProperty("continent", vf.createValue("australia"), kangaroo);
        setProperty("continent", vf.createValue("America"), opossum);

        elephant.getImpersonation().grantImpersonation(jackrabbit.getPrincipal());
        elephant.getImpersonation().grantImpersonation(backslash.getPrincipal());

        authorizables.addAll(users);
        authorizables.addAll(groups);

        if (!userMgr.isAutoSave()) {
            superuser.save();
        }

    }

    @Override
    public void tearDown() throws Exception {
        for (Authorizable authorizable : authorizables) {
            if (!systemDefined.contains(authorizable)) {
                authorizable.remove();
            }
        }
        authorizables.clear();
        groups.clear();
        users.clear();

        if (!userMgr.isAutoSave()) {
            superuser.save();
        }

        super.tearDown();
    }

    @Test
    public void testAny() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) { /* any */ }
        });

        assertSameElements(result, authorizables.iterator());
    }

    @Test
    public void testSelector() throws RepositoryException {
        List<Class<? extends Authorizable>> selectors = new ArrayList<Class<? extends Authorizable>>();
        selectors.add(Authorizable.class);
        selectors.add(Group.class);
        selectors.add(User.class);

        for (final Class<? extends Authorizable> s : selectors) {
            Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
                public <T> void build(QueryBuilder<T> builder) {
                    builder.setSelector(s);
                }
            });

            if (User.class.isAssignableFrom(s)) {
                assertSameElements(result, users.iterator());
            } else if (Group.class.isAssignableFrom(s)) {
                assertSameElements(result, groups.iterator());
            } else {
                assertSameElements(result, authorizables.iterator());
            }
        }
    }

    @Test
    public void testDirectScope() throws RepositoryException {
        Group[] groups = new Group[]{mammals, vertebrates, apes};
        for (final Group g : groups) {
            Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
                public <T> void build(QueryBuilder<T> builder) {
                    try {
                        builder.setScope(g.getID(), true);
                    } catch (RepositoryException e) {
                        fail(e.getMessage());
                    }
                }
            });

            Iterator<Authorizable> members = g.getDeclaredMembers();
            assertSameElements(result, members);
        }
    }

    @Test
    public void testIndirectScope() throws RepositoryException {
        Group[] groups = new Group[]{mammals, vertebrates, apes};
        for (final Group g : groups) {
            Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
                public <T> void build(QueryBuilder<T> builder) {
                    try {
                        builder.setScope(g.getID(), false);
                    } catch (RepositoryException e) {
                        fail(e.getMessage());
                    }
                }
            });

            Iterator<Authorizable> members = g.getMembers();
            assertSameElements(result, members);
        }
    }

    @Test
    public void testFindUsersInGroup() throws RepositoryException {
        Group[] groups = new Group[]{mammals, vertebrates, apes};
        for (final Group g : groups) {
            Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
                public <T> void build(QueryBuilder<T> builder) {
                    try {
                        builder.setSelector(User.class);
                        builder.setScope(g.getID(), false);
                    } catch (RepositoryException e) {
                        fail(e.getMessage());
                    }
                }
            });

            Iterator<Authorizable> members = g.getMembers();
            Iterator<Authorizable> users = Iterators.filter(members, new Predicate<Authorizable>() {
                public boolean apply(Authorizable authorizable) {
                    return !authorizable.isGroup();
                }
            });
            assertSameElements(result, users);
        }
    }

    @Test
    public void testFindGroupsInGroup() throws RepositoryException {
        Group[] groups = new Group[]{mammals, vertebrates, apes};
        for (final Group g : groups) {
            Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
                public <T> void build(QueryBuilder<T> builder) {
                    try {
                        builder.setSelector(Group.class);
                        builder.setScope(g.getID(), true);
                    } catch (RepositoryException e) {
                        fail(e.getMessage());
                    }
                }
            });

            Iterator<Authorizable> members = g.getDeclaredMembers();
            Iterator<Authorizable> users = Iterators.filter(members, new Predicate<Authorizable>() {
                public boolean apply(Authorizable authorizable) {
                    return authorizable.isGroup();
                }
            });
            assertSameElements(result, users);
        }
    }

    @Test
    public void testFindInEveryoneGroup() throws RepositoryException {
        Authorizable everyone = userMgr.getAuthorizable(EveryonePrincipal.NAME);
        boolean doRemove = false;
        try {
            if (everyone == null) {
                everyone = userMgr.createGroup(EveryonePrincipal.NAME);
                superuser.save();
                doRemove = true;
            }

            Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
                public <T> void build(QueryBuilder<T> builder) {
                    builder.setScope(EveryonePrincipal.NAME, true);
                }
            });

            Iterator<Authorizable> members = ((Group) everyone).getDeclaredMembers();
            assertSameElements(result, members);
        } finally {
            if (doRemove) {
                everyone.remove();
                superuser.save();
            }
        }
    }

    @Test
    public void testNameMatch() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.nameMatches("a%"));
            }
        });

        Iterator<Authorizable> expected = Iterators.filter(authorizables.iterator(), new Predicate<Authorizable>() {
            public boolean apply(Authorizable authorizable) {
                try {
                    String name = authorizable.getID();
                    Principal principal = authorizable.getPrincipal();
                    return name.startsWith("a") || principal != null && principal.getName().startsWith("a");
                } catch (RepositoryException e) {
                    fail(e.getMessage());
                }
                return false;
            }
        });

        assertTrue(result.hasNext());
        assertSameElements(result, expected);
    }

    /**
     * The name matching condition must not only search for node-name and
     * principal name but also needs to take the new rep:authoriableId into
     * account that has been introduced as of Oak 1.0
     *
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2243">OAK-2243</a>
     */
    @Test
    public void testNameMatch2() throws RepositoryException {
        // create a user with different id and principal name
        User user = userMgr.createUser("moloch", null, new PrincipalImpl("MolochHorridus"), "reptiles");
        String userPath = user.getPath();
        // move it such that the node name doesn't reveal the id.
        superuser.move(userPath, Text.getRelativeParent(userPath, 1) + "/thorny_dragon");
        superuser.save();
        authorizables.add(user);

        // search for the authorizable ID
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.nameMatches("moloch"));
            }
        });
        assertTrue(result.hasNext());
        Authorizable a = result.next();
        assertEquals("moloch", a.getID());
        assertFalse(result.hasNext());

        // search for the principal name (basically just for backwards compatibility)
        result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.nameMatches("MolochHorridus"));
            }
        });
        assertTrue(result.hasNext());
        a = result.next();
        assertEquals("MolochHorridus", a.getPrincipal().getName());
        assertFalse(result.hasNext());

        // search for the node name
        result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.nameMatches("thorny_dragon"));
            }
        });
        assertTrue(result.hasNext());
        a = result.next();
        assertEquals("thorny_dragon", Text.getName(a.getPath()));
        assertFalse(result.hasNext());
    }

    @Test
    public void testFindProperty1() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        eq("@canFly", vf.createValue(true)));
            }
        });

        Iterator<User> expected = Iterators.filter(users.iterator(), new Predicate<User>() {
            public boolean apply(User user) {
                try {
                    Value[] canFly = user.getProperty("canFly");
                    return canFly != null && canFly.length == 1 && canFly[0].getBoolean();
                } catch (RepositoryException e) {
                    fail(e.getMessage());
                }
                return false;
            }
        });

        assertTrue(result.hasNext());
        assertSameElements(result, expected);
    }

    @Test
    public void testFindProperty2() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        gt("profile/@weight", vf.createValue(2000.0)));
            }
        });

        Iterator<User> expected = Iterators.filter(users.iterator(), new Predicate<User>() {
            public boolean apply(User user) {
                try {
                    Value[] weight = user.getProperty("profile/weight");
                    return weight != null && weight.length == 1 && weight[0].getDouble() > 2000.0;
                } catch (RepositoryException e) {
                    fail(e.getMessage());
                }
                return false;
            }
        });

        assertTrue(result.hasNext());
        assertSameElements(result, expected);
    }

    @Test
    public void testFindProperty3() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        eq("@numberOfLegs", vf.createValue(8)));
            }
        });

        Iterator<User> expected = Iterators.filter(users.iterator(), new Predicate<User>() {
            public boolean apply(User user) {
                try {
                    Value[] numberOfLegs = user.getProperty("numberOfLegs");
                    return numberOfLegs != null && numberOfLegs.length == 1 && numberOfLegs[0].getLong() == 8;
                } catch (RepositoryException e) {
                    fail(e.getMessage());
                }
                return false;
            }
        });

        assertTrue(result.hasNext());
        assertSameElements(result, expected);
    }

    @Test
    public void testPropertyExistence() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        exists("@poisonous"));
            }
        });

        Iterator<User> expected = Iterators.filter(users.iterator(), new Predicate<User>() {
            public boolean apply(User user) {
                try {
                    Value[] poisonous = user.getProperty("poisonous");
                    return poisonous != null && poisonous.length == 1;
                } catch (RepositoryException e) {
                    fail(e.getMessage());
                }
                return false;
            }
        });

        assertTrue(result.hasNext());
        assertSameElements(result, expected);
    }

    @Test
    public void testPropertyLike() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        like("profile/@food", "m%"));
            }
        });

        Iterator<User> expected = Iterators.filter(users.iterator(), new Predicate<User>() {
            public boolean apply(User user) {
                try {
                    Value[] food = user.getProperty("profile/food");
                    if (food == null || food.length != 1) {
                        return false;
                    } else {
                        String value = food[0].getString();
                        return value.startsWith("m");
                    }
                } catch (RepositoryException e) {
                    fail(e.getMessage());
                }
                return false;
            }
        });

        assertTrue(result.hasNext());
        assertSameElements(result, expected);
    }

    @Test
    public void testContains1() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        contains(".", "gold"));
            }
        });

        Iterator<User> expected = Iterators.singletonIterator(goldenToad);
        assertTrue(result.hasNext());
        assertSameElements(result, expected);
    }

    @Test
    public void testContains2() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        contains("@color", "gold"));
            }
        });

        Iterator<User> expected = Iterators.singletonIterator(goldenToad);
        assertTrue(result.hasNext());
        assertSameElements(result, expected);
    }

    @Test
    public void testContains3() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.contains("profile/.", "grass"));
            }
        });

        Iterator<User> expected = Iterators.singletonIterator(kangaroo);
        assertTrue(result.hasNext());
        assertSameElements(result, expected);
    }

    @Test
    public void testContains4() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        contains("profile/@food", "grass"));
            }
        });

        Iterator<User> expected = Iterators.singletonIterator(kangaroo);
        assertTrue(result.hasNext());
        assertSameElements(result, expected);
    }

    @Test
    public void testCondition1() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        and(builder.
                                eq("profile/@cute", vf.createValue(true)), builder.
                                not(builder.
                                        eq("@color", vf.createValue("black")))));
            }
        });

        Iterator<User> expected = Iterators.filter(users.iterator(), new Predicate<User>() {
            public boolean apply(User user) {
                try {
                    Value[] cute = user.getProperty("profile/cute");
                    Value[] black = user.getProperty("color");
                    return cute != null && cute.length == 1 && cute[0].getBoolean() &&
                            !(black != null && black.length == 1 && black[0].getString().equals("black"));
                } catch (RepositoryException e) {
                    fail(e.getMessage());
                }
                return false;
            }
        });

        assertTrue(result.hasNext());
        assertSameElements(result, expected);
    }

    @Test
    public void testCondition2() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        or(builder.
                                eq("profile/@food", vf.createValue("mice")), builder.
                                eq("profile/@food", vf.createValue("nectar"))));
            }
        });

        Iterator<User> expected = Iterators.filter(users.iterator(), new Predicate<User>() {
            public boolean apply(User user) {
                try {
                    Value[] food = user.getProperty("profile/food");
                    return food != null && food.length == 1 &&
                            (food[0].getString().equals("mice") || food[0].getString().equals("nectar"));
                } catch (RepositoryException e) {
                    fail(e.getMessage());
                }
                return false;
            }
        });

        assertTrue(result.hasNext());
        assertSameElements(result, expected);
    }

    @Test
    public void testImpersonation() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        impersonates("jackrabbit"));
            }
        });

        Iterator<User> expected = Iterators.singletonIterator(elephant);
        assertTrue(result.hasNext());
        assertSameElements(expected, result);
    }

    @Test
    public void testImpersonationWithBackslash() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        impersonates("foo\\bar"));
            }
        });

        Iterator<User> expected = Iterators.singletonIterator(elephant);
        assertTrue(result.hasNext());
        assertSameElements(expected, result);
    }

    @Test
    public void testAdminImpersonation() throws Exception {
        final String adminPrincipalName = userMgr.getAuthorizable(superuser.getUserID()).getPrincipal().getName();
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.impersonates(adminPrincipalName));
            }
        });

        Iterator<Authorizable> expected = userMgr.findAuthorizables(UserConstants.REP_PRINCIPAL_NAME, null, UserManager.SEARCH_TYPE_USER);
        assertTrue(result.hasNext());
        assertSameElements(expected, result);
    }

    @Test
    public void testSortOrderIgnoreCaseDescending() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        exists("@color"));
                builder.setSortOrder("@color", QueryBuilder.Direction.DESCENDING, true);
            }
        });

        assertTrue(result.hasNext());
        String prev = null;
        while (result.hasNext()) {
            Authorizable authorizable = result.next();
            Value[] color = authorizable.getProperty("color");
            assertNotNull(color);
            assertEquals(1, color.length);
            assertTrue(prev == null || prev.compareToIgnoreCase(color[0].getString()) >= 0);
            prev = color[0].getString();
        }
    }

    @Test
    public void testSortOrderRespectCaseDescending() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        exists("@color"));
                builder.setSortOrder("@color", QueryBuilder.Direction.DESCENDING, false);
            }
        });

        assertTrue(result.hasNext());
        String prev = null;
        while (result.hasNext()) {
            Authorizable authorizable = result.next();
            Value[] color = authorizable.getProperty("color");
            assertNotNull(color);
            assertEquals(1, color.length);
            assertTrue(prev == null || prev.compareTo(color[0].getString()) >= 0);
            prev = color[0].getString();
        }
    }

    @Test
    public void testSortOrderRespectCaseAscendingDoubleValue() throws RepositoryException {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.
                        exists("profile/@weight"));
                builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING, false);
            }
        });

        assertTrue(result.hasNext());
        double prev = Double.MIN_VALUE;
        while (result.hasNext()) {
            Authorizable authorizable = result.next();
            Value[] weight = authorizable.getProperty("profile/weight");
            assertNotNull(weight);
            assertEquals(1, weight.length);
            assertTrue(prev <= weight[0].getDouble());
            prev = weight[0].getDouble();
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2266">OAK-2266</a>
     */
    @Test
    public void testSortOrderIgnoreCaseAscending() throws Exception {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.exists("@continent"));
                builder.setSortOrder("@continent", QueryBuilder.Direction.ASCENDING, true);
            }
        });

        Iterator<String> continents = ImmutableList.of("africa", "america", "australia").iterator();
        String expected = continents.next();
        while (result.hasNext()) {
            Authorizable a = result.next();
            String continent = a.getProperty("continent")[0].getString().toLowerCase();
            if (!continent.equals(expected)) {
                expected = continents.next();
                assertEquals(expected, continent);
            }
        }
    }

    /**
     * @see <a href="https://issues.apache.org/jira/browse/OAK-2266">OAK-2266</a>
     */
    @Test()
    public void testSortOrderRespectCaseAscending() throws Exception {
        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setCondition(builder.exists("@continent"));
                builder.setSortOrder("@continent", QueryBuilder.Direction.ASCENDING, false);
            }
        });

        Iterator<String> continents = ImmutableList.of("Africa", "America", "africa", "australia").iterator();
        String expected = continents.next();
        while (result.hasNext()) {
            Authorizable a = result.next();
            String continent = a.getProperty("continent")[0].getString();
            if (!continent.equals(expected)) {
                expected = continents.next();
                assertEquals(expected, continent);
            }
        }
    }

    @Test
    public void testOffset() throws RepositoryException {
        long[] offsets = {2, 0, 3, 0, 100000};
        long[] counts = {4, 4, 0, 100000, 100000};

        for (int k = 0; k < offsets.length; k++) {
            final long offset = offsets[k];
            final long count = counts[k];
            Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
                public <T> void build(QueryBuilder<T> builder) {
                    builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING);
                    builder.setLimit(offset, count);
                }
            });

            Iterator<Authorizable> expected = userMgr.findAuthorizables(new Query() {
                public <T> void build(QueryBuilder<T> builder) {
                    builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING);
                }
            });

            skip(expected, offset);
            assertSame(expected, result, count);
            assertFalse(result.hasNext());
        }
    }

    @Test
    public void testSetBound() throws RepositoryException {
        List<User> sortedUsers = new ArrayList<User>(users);
        sortedUsers.removeAll(systemDefined);  // remove system defined users: no @weight

        Comparator<? super User> comp = new Comparator<User>() {
            public int compare(User user1, User user2) {
                try {
                    Value[] weight1 = user1.getProperty("profile/weight");
                    assertNotNull(weight1);
                    assertEquals(1, weight1.length);

                    Value[] weight2 = user2.getProperty("profile/weight");
                    assertNotNull(weight2);
                    assertEquals(1, weight2.length);

                    return weight1[0].getDouble() < weight2[0].getDouble() ? -1 : 1;
                } catch (RepositoryException e) {
                    fail(e.getMessage());
                    return 0;  // Make the compiler happy
                }
            }
        };
        Collections.sort(sortedUsers, comp);

        long[] counts = {4, 0, 100000};
        for (final long count : counts) {
            Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
                public <T> void build(QueryBuilder<T> builder) {
                    builder.setCondition(builder.
                            eq("profile/@cute", vf.createValue(true)));
                    builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING, false);
                    builder.setLimit(vf.createValue(1000.0), count);
                }
            });

            Iterator<User> expected = Iterators.filter(sortedUsers.iterator(), new Predicate<User>() {
                public boolean apply(User user) {
                    try {
                        Value[] cute = user.getProperty("profile/cute");
                        Value[] weight = user.getProperty("profile/weight");
                        return cute != null && cute.length == 1 && cute[0].getBoolean() &&
                                weight != null && weight.length == 1 && weight[0].getDouble() > 1000.0;

                    } catch (RepositoryException e) {
                        fail(e.getMessage());
                    }
                    return false;
                }
            });

            assertSame(expected, result, count);
            assertFalse(result.hasNext());
        }
    }

    @Test
    public void testScopeWithOffset() throws RepositoryException {
        final int offset = 5;
        final int count = 10000;

        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setScope("vertebrates", false);
                builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING);
                builder.setLimit(offset, count);
            }
        });

        Iterator<Authorizable> expected = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setScope("vertebrates", false);
                builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING);
            }
        });

        skip(expected, offset);
        assertSame(expected, result, count);
        assertFalse(result.hasNext());
    }

    @Test
    public void testScopeWithMax() throws RepositoryException {
        final int offset = 0;
        final int count = 22;

        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setScope("vertebrates", false);
                builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING);
                builder.setLimit(offset, count);
            }
        });

        Iterator<Authorizable> expected = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setScope("vertebrates", false);
                builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING);
            }
        });

        assertSameElements(expected, result);
        assertFalse(result.hasNext());
    }

    @Test
    public void testMaxCount() throws Exception {
        final long count = Long.MAX_VALUE;
        final long countjr2 = -1; // in jackrabbit -1 could be used to set no limit

        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING);
                builder.setLimit(0, countjr2);
            }
        });

        Iterator<Authorizable> expected = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING);
                builder.setLimit(0, count);
            }
        });

        assertSame(expected, result, count);
        assertFalse(result.hasNext());
    }

    @Test
    public void testMaxCountWithScope() throws Exception {
        final long count = Long.MAX_VALUE;
        final long countjr2 = -1; // in jackrabbit -1 could be used to set no limit

        Iterator<Authorizable> result = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING);
                builder.setScope("vertebrates", false);
                builder.setLimit(0, countjr2);
            }
        });

        Iterator<Authorizable> expected = userMgr.findAuthorizables(new Query() {
            public <T> void build(QueryBuilder<T> builder) {
                builder.setSortOrder("profile/@weight", QueryBuilder.Direction.ASCENDING);
                builder.setScope("vertebrates", false);
                builder.setLimit(0, count);
            }
        });

        assertSame(expected, result, count);
        assertFalse(result.hasNext());
    }

    @Test
    public void testQueryUserWithSpecialCharId() throws Exception {
        List<String> ids = Lists.newArrayList("'", "]");
        for (String id : ids) {
            User user = null;
            try {
                user = userMgr.createUser(id, "pw");
                superuser.save();

                boolean found = false;
                String query = "{\"condition\":[{\"named\":\"" + id + "\"}]}";
                AuthorizableQueryManager queryManager = new AuthorizableQueryManager(userMgr, superuser.getValueFactory());
                Iterator<Authorizable> it = queryManager.execute(query);
                while (it.hasNext() && !found) {
                    Authorizable a = it.next();
                    found = id.equals(a.getID());
                }
                assertTrue(found);
            } finally {
                if (user != null) {
                    user.remove();
                    superuser.save();
                }
            }
        }
    }



    //------------------------------------------------------------< private >---

    private static void addMembers(Group group, Authorizable... authorizables) throws RepositoryException {
        for (Authorizable authorizable : authorizables) {
            group.addMember(authorizable);
        }
    }

    private Group createGroup(String name) throws RepositoryException {
        Group group = userMgr.createGroup(name);
        groups.add(group);
        return group;
    }

    private User createUser(String name, String food, double weight, boolean cute) throws RepositoryException {
        User user = userMgr.createUser(name, "");
        user.setProperty("profile/food", vf.createValue(food));
        user.setProperty("profile/weight", vf.createValue(weight));
        user.setProperty("profile/cute", vf.createValue(cute));
        users.add(user);
        return user;
    }

    private static void setProperty(String relPath, Value value, Authorizable... authorizables) throws RepositoryException {
        for (Authorizable authorizable : authorizables) {
            authorizable.setProperty(relPath, value);
        }
    }

    private static <T> void assertSameElements(Iterator<? extends T> it1, Iterator<? extends T> it2) {
        Set<? extends T> set1 = toSet(it1);
        Set<? extends T> set2 = toSet(it2);

        Set<? super T> missing = new HashSet<T>();
        missing.addAll(set2);
        missing.removeAll(set1);

        Set<? super T> excess = new HashSet<T>();
        excess.addAll(set1);
        excess.removeAll(set2);

        if (!missing.isEmpty()) {
            fail("Missing elements in query result: " + missing);
        }

        if (!excess.isEmpty()) {
            fail("Excess elements in query result: " + excess);
        }
    }

    private static <T> Set<T> toSet(Iterator<T> it) {
        Set<T> set = new HashSet<T>();
        while (it.hasNext()) {
            set.add(it.next());
        }
        return set;
    }

    private static <T> void assertSame(Iterator<? extends T> expected, Iterator<? extends T> actual, long count) {
        for (int k = 0; k < count && actual.hasNext(); k++) {
            assertTrue(expected.hasNext());
            assertEquals(expected.next(), actual.next());
        }
    }

    private static <T> void skip(Iterator<T> iterator, long count) {
        for (int k = 0; k < count && iterator.hasNext(); k++) {
            iterator.next();
        }
    }
}