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
import java.util.Iterator;
import java.util.Set;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Implementation of the {@code PrincipalProvider} interface that never
 * returns any principals.
 */
public final class EmptyPrincipalProvider implements PrincipalProvider {

    public static final PrincipalProvider INSTANCE = new EmptyPrincipalProvider();

    private EmptyPrincipalProvider() {}

    @Override
    public Principal getPrincipal(@NotNull String principalName) {
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
        return Collections.emptySet();
    }

    @NotNull
    @Override
    public Set<? extends Principal> getPrincipals(@NotNull String userID) {
        return Collections.emptySet();
    }

    @NotNull
    @Override
    public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType) {
        return Collections.emptyIterator();
    }

    @NotNull
    @Override
    public Iterator<? extends Principal> findPrincipals(int searchType) {
        return Collections.emptyIterator();
    }
}
