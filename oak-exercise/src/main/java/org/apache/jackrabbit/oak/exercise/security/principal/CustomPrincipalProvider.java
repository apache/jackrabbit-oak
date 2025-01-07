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
package org.apache.jackrabbit.oak.exercise.security.principal;

import java.security.Principal;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Custom principal provider that only knows of a predefined set of principals
 * and their group membership.
 *
 * EXERCISE: complete the implemenation
 */
class CustomPrincipalProvider implements PrincipalProvider {

    private final Set<String> knownPrincipalNames;

    CustomPrincipalProvider(String[] knownPrincipalNames) {
        this.knownPrincipalNames = Set.of(knownPrincipalNames);
    }

    @Nullable
    @Override
    public Principal getPrincipal(@NotNull String principalName) {
        // EXERCISE: complete
        if (knownPrincipalNames.contains(principalName)) {
            return () -> principalName;
        } else {
            return null;
        }
    }

    @NotNull
    @Override
    public Set<Principal> getMembershipPrincipals(@NotNull Principal principal) {
        // EXERCISE : expose the group membership of your known Principals
        // EXERCISE : add every other principal into one of your known-principal-groups to establish dynamic group membership
        return Collections.emptySet();
    }

    @NotNull
    @Override
    public Set<? extends Principal> getPrincipals(@NotNull String userID) {
        // EXERCISE : expose the principal-sets of your known principals
        // EXERCISE : add every other principal into one of your known-principal-groups to establish dynamic group membership
        return Collections.emptySet();
    }

    @NotNull
    @Override
    public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType) {
        // EXERCISE
        return Collections.emptyIterator();
    }

    @NotNull
    @Override
    public Iterator<? extends Principal> findPrincipals(int searchType) {
        // EXERCISE
        return Collections.emptyIterator();
    }
}
