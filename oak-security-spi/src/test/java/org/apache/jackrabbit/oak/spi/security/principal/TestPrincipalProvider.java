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
package org.apache.jackrabbit.oak.spi.security.principal;

import java.security.Principal;
import java.security.acl.Group;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;

public final class TestPrincipalProvider implements PrincipalProvider {

    public static final Principal UNKNOWN = new PrincipalImpl("unknown");

    private final boolean exposesEveryone;
    private final Map<String, Principal> principals;

    public TestPrincipalProvider() {
        this(true);
    }

    public TestPrincipalProvider(boolean exposesEveryone) {
        this.exposesEveryone = exposesEveryone;
        this.principals = TestPrincipals.asMap();
    }

    public TestPrincipalProvider(String... principalNames) {
        this.exposesEveryone = true;
        this.principals = Maps.toMap(ImmutableSet.copyOf(principalNames), new Function<String, Principal>() {
            @Override
            public Principal apply(String input) {
                return new PrincipalImpl(input);
            }
        });
    }

    public Iterable<Principal> getTestPrincipals() {
        return principals.values();
    }

    public Iterable<Principal> all() {
        Set all = Sets.newHashSet(principals.values());
        all.add(EveryonePrincipal.getInstance());
        return all;
    }

    public static String getIDFromPrincipal(@Nonnull Principal principal) {
        return principal.getName() + "_id";
    }

    @CheckForNull
    private static String getPrincipalNameFromID(@Nonnull String id) {
        if (id.endsWith("_id")) {
            return id.substring(0, id.lastIndexOf("_id"));
        } else {
            return null;
        }
    }

    @CheckForNull
    @Override
    public Principal getPrincipal(@Nonnull String principalName) {
        if (exposesEveryone && EveryonePrincipal.NAME.equals(principalName)) {
            return EveryonePrincipal.getInstance();
        } else {
            return principals.get(principalName);
        }
    }

    @Nonnull
    @Override
    public Set<Group> getGroupMembership(@Nonnull Principal principal) {
        if (principals.equals(TestPrincipals.asMap())) {
            return TestPrincipals.membership(principal.getName());
        } else if (principals.values().contains(principal)) {
            return ImmutableSet.<Group>of(EveryonePrincipal.getInstance());
        } else {
            return ImmutableSet.of();
        }
    }

    @Nonnull
    @Override
    public Set<? extends Principal> getPrincipals(@Nonnull String userID) {
        String pName = getPrincipalNameFromID(userID);
        if (pName != null) {
            Principal p = principals.get(pName);
            if (p != null) {
                Set s = Sets.newHashSet(p);
                s.addAll(getGroupMembership(p));
                return s;
            }
        }

        return ImmutableSet.of();
    }

    @Nonnull
    @Override
    public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType) {
        return Iterables.filter(all(), new SearchTypePredicate(nameHint, searchType)).iterator();
    }

    @Nonnull
    @Override
    public Iterator<? extends Principal> findPrincipals(int searchType) {
        return findPrincipals(null, searchType);
    }

    private static final class SearchTypePredicate implements Predicate<Principal> {

        private final int searchType;
        private final String nameHint;

        private SearchTypePredicate(@Nullable String nameHint, int searchType) {
            this.searchType = searchType;
            this.nameHint = nameHint;
        }

        @Override
        public boolean apply(Principal principal) {
            if (nameHint != null && principal != null && !principal.getName().startsWith(nameHint)) {
                return false;
            }

            switch (searchType) {
                case PrincipalManager.SEARCH_TYPE_ALL: return true;
                case PrincipalManager.SEARCH_TYPE_GROUP: return principal instanceof Group;
                case PrincipalManager.SEARCH_TYPE_NOT_GROUP: return !(principal instanceof Group);
                default: throw new IllegalArgumentException();
            }
        }
    }

    private static final class TestGroup extends PrincipalImpl implements Group {

        private final Enumeration<? extends Principal> members;

        public TestGroup(String name, Principal... members) {
            super(name);
            Set<? extends Principal> mset = ImmutableSet.copyOf(members);
            this.members = Iterators.asEnumeration(mset.iterator());
        }

        @Override
        public boolean addMember(Principal user) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeMember(Principal user) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isMember(Principal member) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Enumeration<? extends Principal> members() {
            return members;
        }
    }

    private static final class TestPrincipals {

        private static final Principal a = new PrincipalImpl("a");
        private static final Principal ac = new PrincipalImpl("ac");
        private static final Group gr1 = new TestGroup("tGr1");
        private static final Group gr2 = new TestGroup("tGr2", a);
        private static final Group gr3 = new TestGroup("gr2", gr2, ac);

        private static final Map<String, Principal> principals = ImmutableMap.<String, Principal>builder()
                .put(a.getName(), a)
                .put("b", new PrincipalImpl("b"))
                .put(ac.getName(), ac)
                .put(gr1.getName(), gr1)
                .put(gr2.getName(), gr2)
                .put(gr3.getName(), gr3).build();

        private static Map<String, Principal> asMap() {
            return principals;
        }

        private static Set<Group> membership(@Nonnull String name) {
            if ("a".equals(name)) {
                return ImmutableSet.of(EveryonePrincipal.getInstance(), gr2, gr3);
            } else if ("ac".equals(name)) {
                return ImmutableSet.of(EveryonePrincipal.getInstance(), gr3);
            } else if (gr2.getName().equals(name)) {
                return ImmutableSet.of(EveryonePrincipal.getInstance(), gr3);
            } else if (principals.containsKey(name)) {
                return ImmutableSet.<Group>of(EveryonePrincipal.getInstance());
            } else {
                return ImmutableSet.of();
            }
        }
    }
}