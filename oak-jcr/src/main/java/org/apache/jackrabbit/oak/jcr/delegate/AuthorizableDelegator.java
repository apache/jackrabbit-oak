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

import static com.google.common.base.Preconditions.checkArgument;

import java.security.Principal;
import java.util.Iterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.jcr.RepositoryException;
import javax.jcr.Value;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

/**
 * Base class for {@link GroupDelegator} and {@link UserDelegator}.
 */
abstract class AuthorizableDelegator implements Authorizable {

    final SessionDelegate sessionDelegate;
    final Authorizable delegate;

    AuthorizableDelegator(@Nonnull SessionDelegate sessionDelegate, @Nonnull Authorizable delegate) {
        checkArgument(!(delegate instanceof AuthorizableDelegator));
        this.sessionDelegate = sessionDelegate;
        this.delegate = delegate;
    }

    static Authorizable wrap(@Nonnull SessionDelegate sessionDelegate, @Nullable Authorizable authorizable) {
        if (authorizable == null) {
            return null;
        }
        if (authorizable.isGroup()) {
            return GroupDelegator.wrap(sessionDelegate, (Group) authorizable);
        } else {
            return UserDelegator.wrap(sessionDelegate, (User) authorizable);
        }
    }

    static Authorizable unwrap(@Nonnull Authorizable authorizable) {
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
            @Nonnull
            @Override
            public Boolean perform() {
                return delegate.isGroup();
            }
        });
    }

    @Override
    public String getID() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<String>("getID") {
            @Nonnull
            @Override
            public String perform() throws RepositoryException {
                return delegate.getID();
            }
        });
    }



    @Override
    public Principal getPrincipal() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Principal>("getPrincipal") {
            @Nonnull
            @Override
            public Principal perform() throws RepositoryException {
                return delegate.getPrincipal();
            }
        });
    }

    @Override
    public Iterator<Group> declaredMemberOf() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Group>>("declaredMemberOf") {
            @Nonnull
            @Override
            public Iterator<Group> perform() throws RepositoryException {
                Iterator<Group> groups = delegate.declaredMemberOf();
                return Iterators.transform(groups, new Function<Group, Group>() {
                    @Nullable
                    @Override
                    public Group apply(@Nullable Group group) {
                        return GroupDelegator.wrap(sessionDelegate, group);
                    }
                });
            }
        });
    }

    @Override
    public Iterator<Group> memberOf() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Group>>("memberOf") {
            @Nonnull
            @Override
            public Iterator<Group> perform() throws RepositoryException {
                Iterator<Group> groups = delegate.memberOf();
                return Iterators.transform(groups, new Function<Group, Group>() {
                    @Nullable
                    @Override
                    public Group apply(@Nullable Group group) {
                        return GroupDelegator.wrap(sessionDelegate, group);
                    }
                });
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

    @Override
    public Iterator<String> getPropertyNames() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<String>>("getPropertyNames") {
            @Nonnull
            @Override
            public Iterator<String> perform() throws RepositoryException {
                return delegate.getPropertyNames();
            }
        });
    }

    @Override
    public Iterator<String> getPropertyNames(final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<String>>("getPropertyNames") {
            @Nonnull
            @Override
            public Iterator<String> perform() throws RepositoryException {
                return delegate.getPropertyNames(relPath);
            }
        });
    }

    @Override
    public boolean hasProperty(final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("hasProperty") {
            @Nonnull
            @Override
            public Boolean perform() throws RepositoryException {
                return delegate.hasProperty(relPath);
            }
        });
    }

    @Override
    public void setProperty(final String relPath, final Value value) throws RepositoryException {
        sessionDelegate.performVoid(new SessionOperation<Void>("setProperty", true) {
            @Override
            public void performVoid() throws RepositoryException {
                delegate.setProperty(relPath, value);
            }
        });
    }

    @Override
    public void setProperty(final String relPath, final Value[] value) throws RepositoryException {
        sessionDelegate.performVoid(new SessionOperation<Void>("setProperty", true) {
            @Override
            public void performVoid() throws RepositoryException {
                delegate.setProperty(relPath, value);
            }
        });
    }

    @Override
    public Value[] getProperty(final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Value[]>("getProperty") {
            @Nonnull
            @Override
            public Value[] perform() throws RepositoryException {
                return delegate.getProperty(relPath);
            }
        });
    }

    @Override
    public boolean removeProperty(final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>("removeProperty", true) {
            @Nonnull
            @Override
            public Boolean perform() throws RepositoryException {
                return delegate.removeProperty(relPath);
            }
        });
    }

    @Override
    public String getPath() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<String>("getPath") {
            @Nonnull
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
