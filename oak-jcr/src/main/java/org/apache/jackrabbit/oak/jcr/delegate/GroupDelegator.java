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
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

/**
 * This implementation of {@code Group} delegates back to a
 * delegatee wrapping each call into a {@link SessionOperation} closure.
 *
 * @see SessionDelegate#perform(SessionOperation)
 */
final class GroupDelegator extends AuthorizableDelegator implements Group {

    private GroupDelegator(SessionDelegate sessionDelegate, Group groupDelegate) {
        super(sessionDelegate, groupDelegate);
    }

    static Group wrap(@Nonnull SessionDelegate sessionDelegate, Group group) {
        if (group == null) {
            return null;
        } else {
            return new GroupDelegator(sessionDelegate, group);
        }
    }

    @Nonnull
    static Group unwrap(@Nonnull Group group) {
        if (group instanceof GroupDelegator) {
            return ((GroupDelegator) group).getDelegate();
        } else {
            return group;
        }
    }

    private Group getDelegate() {
        return (Group) delegate;
    }

    //--------------------------------------------------------------< Group >---
    @Override
    public Iterator<Authorizable> getDeclaredMembers() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Authorizable>>("getDeclaredMembers") {
            @Nonnull
            @Override
            public Iterator<Authorizable> perform() throws RepositoryException {
                Iterator<Authorizable> authorizables = getDelegate().getDeclaredMembers();
                return Iterators.transform(authorizables, new Function<Authorizable, Authorizable>() {
                    @Nullable
                    @Override
                    public Authorizable apply(@Nullable Authorizable authorizable) {
                        return wrap(sessionDelegate, authorizable);
                    }
                });
            }
        });
    }

    @Override
    public Iterator<Authorizable> getMembers() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Authorizable>>("getMembers") {
            @Nonnull
            @Override
            public Iterator<Authorizable> perform() throws RepositoryException {
                Iterator<Authorizable> authorizables = getDelegate().getMembers();
                return Iterators.transform(authorizables, new Function<Authorizable, Authorizable>() {
                    @Nullable
                    @Override
                    public Authorizable apply(@Nullable Authorizable authorizable) {
                        return wrap(sessionDelegate, authorizable);
                    }
                });
            }
        });
    }

    @Override
    public boolean isDeclaredMember(final Authorizable authorizable) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("isDeclaredMember") {
            @Nonnull
            @Override
            public Boolean perform() throws RepositoryException {
                return getDelegate().isDeclaredMember(unwrap(authorizable));
            }
        });
    }

    @Override
    public boolean isMember(final Authorizable authorizable) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("isMember") {
            @Nonnull
            @Override
            public Boolean perform() throws RepositoryException {
                return getDelegate().isMember(unwrap(authorizable));
            }
        });
    }

    @Override
    public boolean addMember(final Authorizable authorizable) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("addMember", true) {
            @Nonnull
            @Override
            public Boolean perform() throws RepositoryException {
                return getDelegate().addMember(unwrap(authorizable));
            }
        });
    }

    @Override
    public Set<String> addMembers(@Nonnull final String... memberIds) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Set<String>>("addMembers", true) {
            @Nonnull
            @Override
            public Set<String> perform() throws RepositoryException {
                return getDelegate().addMembers(memberIds);
            }
        });
    }

    @Override
    public boolean removeMember(final Authorizable authorizable) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("removeMember", true) {
            @Nonnull
            @Override
            public Boolean perform() throws RepositoryException {
                return getDelegate().removeMember(unwrap(authorizable));
            }
        });
    }

    @Override
    public Set<String> removeMembers(@Nonnull final String... memberIds) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Set<String>>("removeMembers", true) {
            @Nonnull
            @Override
            public Set<String> perform() throws RepositoryException {
                return getDelegate().removeMembers(memberIds);
            }
        });
    }
}
