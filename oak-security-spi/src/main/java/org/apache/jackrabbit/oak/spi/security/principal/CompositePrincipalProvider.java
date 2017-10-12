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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.Iterators;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@code PrincipalProvider} implementation that aggregates a list of principal
 * providers into a single.
 */
public class CompositePrincipalProvider implements PrincipalProvider {

    private final List<PrincipalProvider> providers;

    public CompositePrincipalProvider(List<PrincipalProvider> providers) {
        this.providers = checkNotNull(providers);
    }

    public static PrincipalProvider of(@Nonnull List<PrincipalProvider> providers) {
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
    @Override
    public Principal getPrincipal(@Nonnull String principalName) {
        Principal principal = null;
        for (int i = 0; i < providers.size() && principal == null; i++) {
            principal = providers.get(i).getPrincipal(principalName);

        }
        return principal;
    }

    @Nonnull
    @Override
    public Set<Group> getGroupMembership(@Nonnull Principal principal) {
        Set<Group> groups = new HashSet<Group>();
        for (PrincipalProvider provider : providers) {
            groups.addAll(provider.getGroupMembership(principal));
        }
        return groups;
    }

    @Nonnull
    @Override
    public Set<Principal> getPrincipals(@Nonnull String userID) {
        Set<Principal> principals = new HashSet<Principal>();
        for (PrincipalProvider provider : providers) {
            principals.addAll(provider.getPrincipals(userID));
        }
        return principals;
    }

    @Nonnull
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

    @Nonnull
    @Override
    public Iterator<? extends Principal> findPrincipals(int searchType) {
        return findPrincipals(null, searchType);
    }
}