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
import java.util.Enumeration;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.jackrabbit.guava.common.collect.ImmutableSet;
import org.apache.jackrabbit.guava.common.collect.Iterables;
import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.guava.common.collect.Maps;

import org.apache.jackrabbit.api.security.principal.GroupPrincipal;
import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

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
        this.principals = Maps.toMap(ImmutableSet.copyOf(principalNames), input -> new ItemBasedPrincipal() {
            @NotNull
            @Override
            public String getPath() {
                return "/path/to/principal/" + input;
            }

            @Override
            public String getName() {
                return input;
            }
        });
    }

    public Iterable<Principal> getTestPrincipals() {
        return principals.values();
    }

    public Iterable<Principal> all() {
        Set<Principal> all = new LinkedHashSet<>(principals.values());
        all.add(EveryonePrincipal.getInstance());
        return all;
    }

    public static String getIDFromPrincipal(@NotNull Principal principal) {
        return principal.getName() + "_id";
    }

    @Nullable
    private static String getPrincipalNameFromID(@NotNull String id) {
        if (id.endsWith("_id")) {
            return id.substring(0, id.lastIndexOf("_id"));
        } else {
            return null;
        }
    }

    @Nullable
    @Override
    public Principal getPrincipal(@NotNull String principalName) {
        if (exposesEveryone && EveryonePrincipal.NAME.equals(principalName)) {
            return EveryonePrincipal.getInstance();
        } else {
            return principals.get(principalName);
        }
    }

    @NotNull
    @Override
    public Set<Principal> getMembershipPrincipals(@NotNull Principal principal) {
        if (principals.equals(TestPrincipals.asMap())) {
            return TestPrincipals.membership(principal.getName());
        } else if (principals.values().contains(principal)) {
            return Set.of(EveryonePrincipal.getInstance());
        } else {
            return Set.of();
        }
    }

    @NotNull
    @Override
    public Set<? extends Principal> getPrincipals(@NotNull String userID) {
        String pName = getPrincipalNameFromID(userID);
        if (pName != null) {
            Principal p = principals.get(pName);
            if (p != null) {
                Set<Principal> s = new HashSet<>();
                s.add(p);
                s.addAll(getMembershipPrincipals(p));
                return s;
            }
        }

        return Set.of();
    }

    @NotNull
    @Override
    public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType) {
        return Iterables.filter(all(), new SearchTypePredicate(nameHint, searchType)::test).iterator();
    }

    @NotNull
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
        public boolean test(Principal principal) {
            if (nameHint != null && principal != null && !principal.getName().startsWith(nameHint)) {
                return false;
            }

            switch (searchType) {
                case PrincipalManager.SEARCH_TYPE_ALL: return true;
                case PrincipalManager.SEARCH_TYPE_GROUP: return principal instanceof GroupPrincipal;
                case PrincipalManager.SEARCH_TYPE_NOT_GROUP: return !(principal instanceof GroupPrincipal);
                default: throw new IllegalArgumentException();
            }
        }
    }

    private static final class TestGroup extends PrincipalImpl implements GroupPrincipal {

        private final Enumeration<? extends Principal> members;

        public TestGroup(String name, Principal... members) {
            super(name);
            Set<? extends Principal> mset = ImmutableSet.copyOf(members);
            this.members = Iterators.asEnumeration(mset.iterator());
        }

        @Override
        public boolean isMember(@NotNull Principal member) {
            throw new UnsupportedOperationException();
        }

        @NotNull
        @Override
        public Enumeration<? extends Principal> members() {
            return members;
        }
    }

    private static final class TestPrincipals {

        private static final Principal a = new PrincipalImpl("a");
        private static final Principal ac = new PrincipalImpl("ac");
        private static final GroupPrincipal gr1 = new TestGroup("tGr1");
        private static final GroupPrincipal gr2 = new TestGroup("tGr2", a);
        private static final GroupPrincipal gr3 = new TestGroup("gr2", gr2, ac);

        private static final Map<String, Principal> principals = Map.of(
                a.getName(), a,
                "b", new PrincipalImpl("b"),
                ac.getName(), ac,
                gr1.getName(), gr1,
                gr2.getName(), gr2,
                gr3.getName(), gr3);

        private static Map<String, Principal> asMap() {
            return principals;
        }

        private static Set<Principal> membership(@NotNull String name) {
            if ("a".equals(name)) {
                return Set.of(EveryonePrincipal.getInstance(), gr2, gr3);
            } else if ("ac".equals(name)) {
                return Set.of(EveryonePrincipal.getInstance(), gr3);
            } else if (gr2.getName().equals(name)) {
                return Set.of(EveryonePrincipal.getInstance(), gr3);
            } else if (principals.containsKey(name)) {
                return Set.of(EveryonePrincipal.getInstance());
            } else {
                return Set.of();
            }
        }
    }
}
