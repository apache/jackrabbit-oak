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

import org.apache.jackrabbit.guava.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.jcr.RepositoryException;
import javax.jcr.Value;
import java.security.Principal;
import java.util.Iterator;
import java.util.Objects;

import static org.apache.jackrabbit.oak.commons.conditions.Validate.checkArgument;

/**
 * Base class for {@link GroupDelegator} and {@link UserDelegator}.
 */
abstract class AuthorizableDelegator implements Authorizable {

    final SessionDelegate sessionDelegate;
    final Authorizable delegate;

    AuthorizableDelegator(@NotNull SessionDelegate sessionDelegate, @NotNull Authorizable delegate) {
        checkArgument(!(delegate instanceof AuthorizableDelegator));
        this.sessionDelegate = sessionDelegate;
        this.delegate = delegate;
    }

    @Nullable
    static Authorizable wrap(@NotNull SessionDelegate sessionDelegate, @Nullable Authorizable authorizable) {
        if (authorizable == null) {
            return null;
        }
        if (authorizable.isGroup()) {
            return GroupDelegator.wrap(sessionDelegate, (Group) authorizable);
        } else {
            return UserDelegator.wrap(sessionDelegate, (User) authorizable);
        }
    }

    @NotNull
    static Authorizable unwrap(@NotNull Authorizable authorizable) {
        if (authorizable.isGroup()) {
            return GroupDelegator.unwrap((Group) authorizable);
        } else {
            return UserDelegator.unwrap((User) authorizable);
        }
    }

    //-------------------------------------------------------< Authorizable >---
    @Override
    public boolean isGroup() {
        return sessionDelegate.safePerform(new SessionOperation<Boolean>("isGroup") {
            @NotNull
            @Override
            public Boolean perform() {
                return delegate.isGroup();
            }
        });
    }

    @NotNull
    @Override
    public String getID() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<String>("getID") {
            @NotNull
            @Override
            public String perform() throws RepositoryException {
                return delegate.getID();
            }
        });
    }



    @NotNull
    @Override
    public Principal getPrincipal() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Principal>("getPrincipal") {
            @NotNull
            @Override
            public Principal perform() throws RepositoryException {
                return delegate.getPrincipal();
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Group> declaredMemberOf() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Group>>("declaredMemberOf") {
            @NotNull
            @Override
            public Iterator<Group> perform() throws RepositoryException {
                Iterator<Group> groups = delegate.declaredMemberOf();
                return Iterators.transform(Iterators.filter(groups, Objects::nonNull), group -> GroupDelegator.wrap(sessionDelegate, group));
            }
        });
    }

    @NotNull
    @Override
    public Iterator<Group> memberOf() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Group>>("memberOf") {
            @NotNull
            @Override
            public Iterator<Group> perform() throws RepositoryException {
                Iterator<Group> groups = delegate.memberOf();
                return Iterators.transform(Iterators.filter(groups, Objects::nonNull), group -> GroupDelegator.wrap(sessionDelegate, group));
            }
        });
    }

    @Override
    public void remove() throws RepositoryException {
        sessionDelegate.performVoid(new SessionOperation<Void>("remove", true) {
            @Override
            public void performVoid() throws RepositoryException {
                delegate.remove();
            }
        });
    }

    @NotNull
    @Override
    public Iterator<String> getPropertyNames() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<String>>("getPropertyNames") {
            @NotNull
            @Override
            public Iterator<String> perform() throws RepositoryException {
                return delegate.getPropertyNames();
            }
        });
    }

    @NotNull
    @Override
    public Iterator<String> getPropertyNames(@NotNull final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<String>>("getPropertyNames") {
            @NotNull
            @Override
            public Iterator<String> perform() throws RepositoryException {
                return delegate.getPropertyNames(relPath);
            }
        });
    }

    @Override
    public boolean hasProperty(@NotNull final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("hasProperty") {
            @NotNull
            @Override
            public Boolean perform() throws RepositoryException {
                return delegate.hasProperty(relPath);
            }
        });
    }

    @Override
    public void setProperty(@NotNull final String relPath, @Nullable final Value value) throws RepositoryException {
        sessionDelegate.performVoid(new SessionOperation<Void>("setProperty", true) {
            @Override
            public void performVoid() throws RepositoryException {
                delegate.setProperty(relPath, value);
            }
        });
    }

    @Override
    public void setProperty(@NotNull final String relPath, @Nullable final Value[] value) throws RepositoryException {
        sessionDelegate.performVoid(new SessionOperation<Void>("setProperty", true) {
            @Override
            public void performVoid() throws RepositoryException {
                delegate.setProperty(relPath, value);
            }
        });
    }

    @Nullable
    @Override
    public Value[] getProperty(@NotNull final String relPath) throws RepositoryException {
        return sessionDelegate.performNullable(new SessionOperation<Value[]>("getProperty") {
            @Override
            public Value[] performNullable() throws RepositoryException {
                return delegate.getProperty(relPath);
            }
        });
    }

    @Override
    public boolean removeProperty(@NotNull final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("removeProperty", true) {
            @NotNull
            @Override
            public Boolean perform() throws RepositoryException {
                return delegate.removeProperty(relPath);
            }
        });
    }

    @NotNull
    @Override
    public String getPath() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<String>("getPath") {
            @NotNull
            @Override
            public String perform() throws RepositoryException {
                return delegate.getPath();
            }
        });
    }

    //-------------------------------------------------------------< Object >---
    @Override
    public boolean equals(Object other) {
        if (other == this) {
            return true;
        }
        if (other instanceof AuthorizableDelegator) {
            AuthorizableDelegator ad = (AuthorizableDelegator) other;
            return delegate.equals(ad.delegate);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }


    @Override
    public String toString() {
        return delegate.toString();
    }
}
