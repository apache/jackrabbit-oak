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
package org.apache.jackrabbit.oak.spi.security.user;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;

import java.util.Collections;
import java.util.Iterator;

public interface DynamicMembershipProvider {

    /**
     * Returns {@code true} if this implementation of {@link DynamicMembershipProvider} covers all members for the given 
     * {@link Group} making it a fully dynamic group.
     * 
     * @param group The target group
     * @return {@code true} if the provider covers all members of the given target group i.e. making it a fully dynamic group (like for example the 'everyone' group); {@code false} otherwise.
     */
    boolean coversAllMembers(@NotNull Group group);

    /**
     * Returns the dynamic members for the given group.
     *
     * @param group The target group.
     * @param includeInherited If {@code true} inherited members should be included in the resulting iterator.
     * @return An iterator of user/groups that are dynamic members of the given target group.
     * @throws RepositoryException If an error occurs.
     */
    @NotNull Iterator<Authorizable> getMembers(@NotNull Group group, boolean includeInherited) throws RepositoryException;

    /**
     * Returns {@code true} if the given {@code authorizable} is a dynamic member of the given target group.
     * @param group The target group.
     * @param authorizable The user/group that may or may not be dynamic member of the given target group.
     * @param includeInherited If set to {@code true} inherited group membership will be evaluated.
     * @return {@code true} if the given {@code authorizable} is a dynamic member of the given target group.
     * @throws RepositoryException If an error occurs.
     */
    boolean isMember(@NotNull Group group, @NotNull Authorizable authorizable, boolean includeInherited) throws RepositoryException;

    /**
     * Returns an iterator over all groups the given {@code authorizable} is a dynamic member of.
     * @param authorizable The target user/group for which to evaluate membership.
     * @param includeInherited If set to {@code true} inherited group membership will be included in the result.
     * @return An iterator over all groups the given {@code authorizable} is a dynamic member of.
     * @throws RepositoryException If an error occurs.
     */
    @NotNull Iterator<Group> getMembership(@NotNull Authorizable authorizable, boolean includeInherited) throws RepositoryException;

    DynamicMembershipProvider EMPTY = new DynamicMembershipProvider() {

        @Override
        public boolean coversAllMembers(@NotNull Group group) {
            return false;
        }

        @Override
        public @NotNull Iterator<Authorizable> getMembers(@NotNull Group group, boolean includeInherited) {
            return Collections.emptyIterator();
        }

        @Override
        public boolean isMember(@NotNull Group group, @NotNull Authorizable authorizable, boolean includeInherited) {
            return false;
        }

        @Override
        public @NotNull Iterator<Group> getMembership(@NotNull Authorizable authorizable, boolean includeInherited) {
            return Collections.emptyIterator();
        }
    };
}