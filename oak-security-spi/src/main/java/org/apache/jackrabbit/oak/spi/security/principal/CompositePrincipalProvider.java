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
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import com.google.common.collect.Iterators;

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.jackrabbit.api.security.principal.ItemBasedPrincipal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * {@code PrincipalProvider} implementation that aggregates a list of principal
 * providers into a single.
 */
public class CompositePrincipalProvider implements PrincipalProvider {

    private final List<PrincipalProvider> providers;

    public CompositePrincipalProvider(List<PrincipalProvider> providers) {
        this.providers = checkNotNull(providers);
    }

    public static PrincipalProvider of(@NotNull List<PrincipalProvider> providers) {
        PrincipalProvider pp;
        switch (providers.size()) {
            case 0 :
                pp = EmptyPrincipalProvider.INSTANCE;
                break;
            case 1 :
                pp = providers.get(0);
                break;
            default :
                pp = new CompositePrincipalProvider(providers);
        }
        return pp;
    }

    //--------------------------------------------------< PrincipalProvider >---
    @Nullable
    @Override
    public Principal getPrincipal(@NotNull String principalName) {
        Principal principal = null;
        for (int i = 0; i < providers.size() && principal == null; i++) {
            principal = providers.get(i).getPrincipal(principalName);

        }
        return principal;
    }

    @Nullable
    @Override
    public ItemBasedPrincipal getItemBasedPrincipal(@NotNull String principalOakPath) {
        for (PrincipalProvider provider : providers) {
            ItemBasedPrincipal principal = provider.getItemBasedPrincipal(principalOakPath);
            if (principal != null) {
                return principal;
            }
        }
        return null;
    }

    @NotNull
    @Override
    public Set<Group> getGroupMembership(@NotNull Principal principal) {
        return Collections.emptySet();
    }

    @NotNull
    @Override
    public Set<Principal> getMembershipPrincipals(@NotNull Principal principal) {
        Set<Principal> groups = new HashSet<>();
        for (PrincipalProvider provider : providers) {
            groups.addAll(provider.getMembershipPrincipals(principal));
        }
        return groups;
    }

    @NotNull
    @Override
    public Set<Principal> getPrincipals(@NotNull String userID) {
        Set<Principal> principals = new HashSet<>();
        for (PrincipalProvider provider : providers) {
            principals.addAll(provider.getPrincipals(userID));
        }
        return principals;
    }

    @NotNull
    @Override
    public Iterator<Principal> findPrincipals(@Nullable String nameHint, int searchType) {
        Iterator<? extends Principal>[] iterators = new Iterator[providers.size()];
        int i = 0;
        for (PrincipalProvider provider : providers) {
            if (nameHint == null) {
                iterators[i++] = provider.findPrincipals(searchType);
            } else {
                iterators[i++] = provider.findPrincipals(nameHint, searchType);
            }
        }
        return Iterators.concat(iterators);
    }

    @NotNull
    @Override
    public Iterator<? extends Principal> findPrincipals(int searchType) {
        return findPrincipals(null, searchType);
    }

    @NotNull
    @Override
    public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, boolean fullText, int searchType,
            long offset, long limit) {

        List<Iterator<? extends Principal>> all = providers.stream()
                .map((p) -> p.findPrincipals(nameHint, fullText, searchType, 0, limit + offset)).collect(Collectors.toList());
        Iterator<? extends Principal> principals = Iterators.mergeSorted(all, Comparator.comparing(Principal::getName));

        Spliterator<? extends Principal> spliterator = Spliterators.spliteratorUnknownSize(principals, 0);
        Stream<? extends Principal> stream = StreamSupport.stream(spliterator, false);
        if (offset > 0) {
            stream = stream.skip(offset);
        }
        if (limit >= 0) {
            stream = stream.limit(limit);
        }
        return stream.iterator();
    }
}
