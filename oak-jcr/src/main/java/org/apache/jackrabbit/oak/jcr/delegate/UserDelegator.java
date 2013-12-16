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

import static com.google.common.base.Preconditions.checkState;

import java.security.Principal;
import java.util.Iterator;

import javax.annotation.Nullable;
import javax.jcr.Credentials;
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.Impersonation;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

/**
 * This implementation of {@code User} delegates back to a
 * delegatee wrapping each call into a {@link SessionOperation} closure.
 *
 * @see SessionDelegate#perform(SessionOperation)
 */
public final class UserDelegator implements User {
    private final SessionDelegate sessionDelegate;
    private final User userDelegate;

    public UserDelegator(SessionDelegate sessionDelegate, User userDelegate) {
        checkState(!(userDelegate instanceof UserDelegator));
        this.sessionDelegate = sessionDelegate;
        this.userDelegate = userDelegate;
    }

    @Override
    public boolean equals(Object other) {
        return other.equals(this.userDelegate);
    }

    @Override
    public boolean isAdmin() {
        return sessionDelegate.safePerform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() {
                return userDelegate.isAdmin();
            }
        });
    }

    @Override
    public Credentials getCredentials() throws RepositoryException {
        return sessionDelegate.safePerform(new SessionOperation<Credentials>() {
            @Override
            public Credentials perform() throws RepositoryException {
                return userDelegate.getCredentials();
            }
        });
    }

    @Override
    public Impersonation getImpersonation() throws RepositoryException {
        return sessionDelegate.safePerform(new SessionOperation<Impersonation>() {
            @Override
            public Impersonation perform() throws RepositoryException {
                Impersonation impersonation = userDelegate.getImpersonation();
                return new ImpersonationDelegator(sessionDelegate, impersonation);
            }
        });
    }

    @Override
    public void changePassword(final String password) throws RepositoryException {
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                userDelegate.changePassword(password);
                return null;
            }
        });
    }

    @Override
    public void changePassword(final String password, final String oldPassword) throws RepositoryException {
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                userDelegate.changePassword(password, oldPassword);
                return null;
            }
        });
    }

    @Override
    public void disable(final String reason) throws RepositoryException {
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                userDelegate.disable(reason);
                return null;
            }
        });
    }

    @Override
    public boolean isDisabled() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return userDelegate.isDisabled();
            }
        });
    }

    @Override
    public String getDisabledReason() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                return userDelegate.getDisabledReason();
            }
        });
    }

    @Override
    public String getID() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                return userDelegate.getID();
            }
        });
    }

    @Override
    public boolean isGroup() {
        return sessionDelegate.safePerform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() {
                return userDelegate.isGroup();
            }
        });
    }

    @Override
    public Principal getPrincipal() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Principal>() {
            @Override
            public Principal perform() throws RepositoryException {
                return userDelegate.getPrincipal();
            }
        });
    }

    @Override
    public Iterator<Group> declaredMemberOf() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Group>>() {
            @Override
            public Iterator<Group> perform() throws RepositoryException {
                Iterator<Group> groups = userDelegate.declaredMemberOf();
                return Iterators.transform(groups, new Function<Group, Group>() {
                    @Nullable
                    @Override
                    public Group apply(@Nullable Group group) {
                        return new GroupDelegator(sessionDelegate, group);
                    }
                });
            }
        });
    }

    @Override
    public Iterator<Group> memberOf() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Group>>() {
            @Override
            public Iterator<Group> perform() throws RepositoryException {
                Iterator<Group> groups = userDelegate.memberOf();
                return Iterators.transform(groups, new Function<Group, Group>() {
                    @Nullable
                    @Override
                    public Group apply(@Nullable Group group) {
                        return new GroupDelegator(sessionDelegate, group);
                    }
                });
            }
        });
    }

    @Override
    public void remove() throws RepositoryException {
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                userDelegate.remove();
                return null;
            }
        });
    }

    @Override
    public Iterator<String> getPropertyNames() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<String>>() {
            @Override
            public Iterator<String> perform() throws RepositoryException {
                return userDelegate.getPropertyNames();
            }
        });
    }

    @Override
    public Iterator<String> getPropertyNames(final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<String>>() {
            @Override
            public Iterator<String> perform() throws RepositoryException {
                return userDelegate.getPropertyNames(relPath);
            }
        });
    }

    @Override
    public boolean hasProperty(final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return userDelegate.hasProperty(relPath);
            }
        });
    }

    @Override
    public void setProperty(final String relPath, final Value value) throws RepositoryException {
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                userDelegate.setProperty(relPath, value);
                return null;
            }
        });
    }

    @Override
    public void setProperty(final String relPath, final Value[] value) throws RepositoryException {
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                userDelegate.setProperty(relPath, value);
                return null;
            }
        });
    }

    @Override
    public Value[] getProperty(final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Value[]>() {
            @Override
            public Value[] perform() throws RepositoryException {
                return userDelegate.getProperty(relPath);
            }
        });
    }

    @Override
    public boolean removeProperty(final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return userDelegate.removeProperty(relPath);
            }
        });
    }

    @Override
    public String getPath() throws UnsupportedRepositoryOperationException, RepositoryException {
        return sessionDelegate.perform(new SessionOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                return userDelegate.getPath();
            }
        });
    }
}
