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
import java.security.acl.Group;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalProvider;

/**
 * Custom principal provider that only knows of a predefined set of principals
 * and their group membership.
 *
 * EXERCISE: complete the implemenation
 */
class CustomPrincipalProvider implements PrincipalProvider {

    private final Set knownPrincipalNames;

    CustomPrincipalProvider(String[] knownPrincipalNames) {
        this.knownPrincipalNames = ImmutableSet.copyOf(knownPrincipalNames);
    }

    @Override
    public Principal getPrincipal(@Nonnull String principalName) {
        // EXERCISE: complete
        return null;
    }

    @Nonnull
    @Override
    public Set<Group> getGroupMembership(@Nonnull Principal principal) {
        // EXERCISE : expose the group membership of your known Principals
        // EXERCISE : add every other principal into one of your known-principal-groups to establish dynamic group membership
        return Collections.EMPTY_SET;
    }

    @Nonnull
    @Override
    public Set<? extends Principal> getPrincipals(@Nonnull String userID) {
        // EXERCISE : expose the principal-sets of your known principals
        // EXERCISE : add every other principal into one of your known-principal-groups to establish dynamic group membership
        return Collections.EMPTY_SET;
    }

    @Nonnull
    @Override
    public Iterator<? extends Principal> findPrincipals(@Nullable String nameHint, int searchType) {
        // EXERCISE
        return Iterators.emptyIterator();
    }

    @Nonnull
    @Override
    public Iterator<? extends Principal> findPrincipals(int searchType) {
        // EXERCISE
        return Iterators.emptyIterator();
    }
}