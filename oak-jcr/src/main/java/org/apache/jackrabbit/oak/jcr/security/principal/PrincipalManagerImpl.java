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
package org.apache.jackrabbit.oak.jcr.security.principal;

import java.security.Principal;

import org.apache.jackrabbit.api.security.principal.PrincipalIterator;
import org.apache.jackrabbit.api.security.principal.PrincipalManager;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalIteratorAdapter;
import org.apache.jackrabbit.oak.spi.security.principal.EveryonePrincipal;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;

/**
 * PrincipalManagerImpl...
 */
public class PrincipalManagerImpl implements PrincipalManager {

    private final PrincipalProvider principalProvider;

    public PrincipalManagerImpl(PrincipalProvider principalProvider) {
        this.principalProvider = principalProvider;
    }

    @Override
    public boolean hasPrincipal(String principalName) {
        return principalProvider.getPrincipal(principalName) != null;
    }

    @Override
    public Principal getPrincipal(String principalName) {
        return principalProvider.getPrincipal(principalName);
    }

    @Override
    public PrincipalIterator findPrincipals(String simpleFilter) {
        return findPrincipals(simpleFilter, PrincipalManager.SEARCH_TYPE_ALL);
    }

    @Override
    public PrincipalIterator findPrincipals(String simpleFilter, int searchType) {
        return new PrincipalIteratorAdapter(principalProvider.findPrincipals(simpleFilter, searchType));
    }

    @Override
    public PrincipalIterator getPrincipals(int searchType) {
        throw new UnsupportedOperationException("not implemented");
    }

    @Override
    public PrincipalIterator getGroupMembership(Principal principal) {
        return new PrincipalIteratorAdapter(principalProvider.getGroupMembership(principal));
    }

    @Override
    public Principal getEveryone() {
        Principal everyone = getPrincipal(EveryonePrincipal.NAME);
        if (everyone == null) {
            everyone = EveryonePrincipal.getInstance();
        }
        return everyone;
    }
}