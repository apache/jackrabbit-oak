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
package org.apache.jackrabbit.api.security.user;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

import java.util.Iterator;
import java.util.Set;

import org.jetbrains.annotations.NotNull;

/**
 * A Group is a collection of {@link #getMembers() Authorizable}s.
 */
public interface Group extends Authorizable {

    /**
     * @return Iterator of <code>Authorizable</code>s which are declared
     * members of this Group.
     * @throws RepositoryException If an error occurs.
     */
    @NotNull
    Iterator<Authorizable> getDeclaredMembers() throws RepositoryException;

    /**
     * @return Iterator of <code>Authorizable</code>s which are members of
     * this Group. This includes both declared members and all authorizables
     * that are indirect group members.
     * @throws RepositoryException If an error occurs.
     */
    @NotNull
    Iterator<Authorizable> getMembers() throws RepositoryException;

    /**
     * Test whether an {@link Authorizable} is a declared member of this group.
     * @param authorizable  The <code>Authorizable</code> to test.
     * @return  <code>true</code> if the Authorizable to test is a direct member
     * @throws RepositoryException  If an error occurs.
     */
    boolean isDeclaredMember(@NotNull Authorizable authorizable) throws RepositoryException;
    
    /**
     * @param authorizable The <code>Authorizable</code> to test.
     * @return true if the Authorizable to test is a direct or indirect member
     * of this Group.
     * @throws RepositoryException If an error occurs.
     */
    boolean isMember(@NotNull Authorizable authorizable) throws RepositoryException;

    /**
     * Add a member to this Group.
     *
     * @param authorizable The <code>Authorizable</code> to be added as
     * member to this group.
     * @return true if the <code>Authorizable</code> has successfully been added
     * to this Group, false otherwise (e.g. unknown implementation
     * or if it already is a member or if the passed authorizable is this
     * group itself or for some implementation specific constraint).
     * @throws RepositoryException If an error occurs.
     */
    boolean addMember(@NotNull Authorizable authorizable) throws RepositoryException;

    /**
     * Add one or more member(s) to this Group. Note, that an implementation may
     * define circumstances under which this method allows to add non-existing
     * {@code Authorizable}s as new members. Also an implementation may choose to
     * (partially) postpone validation/verification util {@link Session#save()}.
     *
     * @param memberIds The {@code Id}s of the authorizables to be added as
     * members to this group.
     * @return a set of those {@code memberIds} that could not be added or an
     * empty set of all ids have been successfully processed. The former may include
     * those cases where a given id cannot be resolved to an existing authorizable,
     * one that is already member or if adding the member would create a
     * cyclic group membership.
     * @throws RepositoryException If one of the specified memberIds is invalid or
     * if some other error occurs.
     */
    @NotNull
    Set<String> addMembers(@NotNull String... memberIds) throws RepositoryException;

    /**
     * Remove a member from this Group.
     *
     * @param authorizable The <code>Authorizable</code> to be removed from
     * the list of group members.
     * @return true if the Authorizable was successfully removed. False otherwise.
     * @throws RepositoryException If an error occurs.
     */
    boolean removeMember(@NotNull Authorizable authorizable) throws RepositoryException;

    /**
     * Remove one or several members from this Group. Note, that an implementation
     * may define circumstances under which this method allows to remove members
     * that (no longer) exist. An implementation may choose to (partially)
     * postpone validation/verification util {@link Session#save()}.
     *
     * @param memberIds The {@code Id}s of the authorizables to be removed
     * from the members of this group.
     * @return a set of those {@code memberIds} that could not be removed or an
     * empty set if all ids have been successfully processed. The former may include
     * those cases where a given id cannot be resolved to an existing authorizable.
     * @throws RepositoryException If one of the specified memberIds is invalid
     * or if some other error occurs.
     */
    @NotNull
    Set<String> removeMembers(@NotNull String... memberIds) throws RepositoryException;
}
