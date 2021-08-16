/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.jcr.delegate;

import java.util.Iterator;
import java.util.Set;
import javax.jcr.RepositoryException;

import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.jetbrains.annotations.NotNull;

/**
 * This implementation of {@code Group} delegates back to a
 * delegatee wrapping each call into a {@link SessionOperation} closure.
 *
 * @see SessionDelegate#perform(SessionOperation)
 */
final class GroupDelegator extends AuthorizableDelegator implements Group {

    private GroupDelegator(@NotNull SessionDelegate sessionDelegate, @NotNull Group groupDelegate) {
        super(sessionDelegate, groupDelegate);
    }

    @NotNull
    static Group wrap(@NotNull SessionDelegate sessionDelegate, @NotNull Group group) {
        return new GroupDelegator(sessionDelegate, group);
    }

    @NotNull
    static Group unwrap(@NotNull Group group) {
        if (group instanceof GroupDelegator) {
            return ((GroupDelegator) group).getDelegate();
        } else {
            return group;
        }
    }

    @NotNull
    private Group getDelegate() {
        return (Group) delegate;
    }

    //--------------------------------------------------------------< Group >---
    @NotNull
    @Override
    public Iterator<Authorizable> getDeclaredMembers() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Authorizable>>("getDeclaredMembers") {
            @NotNull
            @Override
            public Iterator<Authorizable> perform() throws RepositoryException {
                Iterator<Authorizable> authorizables = getDelegate().getDeclaredMembers();
                return Iterators.transform(authorizables, authorizable -> wrap(sessionDelegate, authorizable));
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Authorizable> getMembers() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Authorizable>>("getMembers") {
            @NotNull
            @Override
            public Iterator<Authorizable> perform() throws RepositoryException {
                Iterator<Authorizable> authorizables = getDelegate().getMembers();
                return Iterators.transform(authorizables, authorizable -> wrap(sessionDelegate, authorizable));
            }
        });
    }

    @Override
    public boolean isDeclaredMember(@NotNull final Authorizable authorizable) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("isDeclaredMember") {
            @NotNull
            @Override
            public Boolean perform() throws RepositoryException {
                return getDelegate().isDeclaredMember(unwrap(authorizable));
            }
        });
    }

    @Override
    public boolean isMember(@NotNull final Authorizable authorizable) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("isMember") {
            @NotNull
            @Override
            public Boolean perform() throws RepositoryException {
                return getDelegate().isMember(unwrap(authorizable));
            }
        });
    }

    @Override
    public boolean addMember(@NotNull final Authorizable authorizable) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("addMember", true) {
            @NotNull
            @Override
            public Boolean perform() throws RepositoryException {
                return getDelegate().addMember(unwrap(authorizable));
            }
        });
    }

    @NotNull
    @Override
    public Set<String> addMembers(@NotNull final String... memberIds) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Set<String>>("addMembers", true) {
            @NotNull
            @Override
            public Set<String> perform() throws RepositoryException {
                return getDelegate().addMembers(memberIds);
            }
        });
    }

    @Override
    public boolean removeMember(@NotNull final Authorizable authorizable) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("removeMember", true) {
            @NotNull
            @Override
            public Boolean perform() throws RepositoryException {
                return getDelegate().removeMember(unwrap(authorizable));
            }
        });
    }

    @NotNull
    @Override
    public Set<String> removeMembers(@NotNull final String... memberIds) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Set<String>>("removeMembers", true) {
            @NotNull
            @Override
            public Set<String> perform() throws RepositoryException {
                return getDelegate().removeMembers(memberIds);
            }
        });
    }
}
