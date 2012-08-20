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
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@code PrincipalProvider} implementation that aggregates a list of principal
 * providers into a single.
 */
public class CompositePrincipalProvider implements PrincipalProvider {

    private static final Logger log = LoggerFactory.getLogger(CompositePrincipalProvider.class);

    private final List<PrincipalProvider> providers;

    public CompositePrincipalProvider(List<PrincipalProvider> providers) {
        assert providers != null;
        this.providers = providers;
    }

    //--------------------------------------------------< PrincipalProvider >---
    @Override
    public Principal getPrincipal(String principalName) {
        Principal principal = null;
        for (int i = 0; i < providers.size() && principal == null; i++) {
            principal = providers.get(i).getPrincipal(principalName);

        }
        return principal;
    }

    @Override
    public Set<Group> getGroupMembership(Principal principal) {
        Set<Group> groups = new HashSet<Group>();
        for (PrincipalProvider provider : providers) {
            groups.addAll(provider.getGroupMembership(principal));
        }
        return groups;
    }

    @Override
    public Set<Principal> getPrincipals(String userID) {
        Set<Principal> principals = new HashSet<Principal>();
        for (PrincipalProvider provider : providers) {
            principals.addAll(provider.getPrincipals(userID));
        }
        return principals;
    }
}