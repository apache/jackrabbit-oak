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
import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.Value;

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
public final class GroupDelegator implements Group {
    private final SessionDelegate sessionDelegate;
    private final Group groupDelegate;

    public GroupDelegator(SessionDelegate sessionDelegate, Group groupDelegate) {
        checkState(!(groupDelegate instanceof GroupDelegator));
        this.sessionDelegate = sessionDelegate;
        this.groupDelegate = groupDelegate;
    }

    @Override
    public boolean equals(Object other) {
        return other.equals(this.groupDelegate);
    }

    @Override
    public Iterator<Authorizable> getDeclaredMembers() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Authorizable>>() {
            @Override
            public Iterator<Authorizable> perform() throws RepositoryException {
                Iterator<Authorizable> authorizables = groupDelegate.getDeclaredMembers();
                return Iterators.transform(authorizables, new Function<Authorizable, Authorizable>() {
                    @Nullable
                    @Override
                    public Authorizable apply(@Nullable Authorizable authorizable) {
                        return AuthorizableDelegator.wrap(sessionDelegate, authorizable);
                    }
                });
            }
        });
    }

    @Override
    public Iterator<Authorizable> getMembers() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Authorizable>>() {
            @Override
            public Iterator<Authorizable> perform() throws RepositoryException {
                Iterator<Authorizable> authorizables = groupDelegate.getMembers();
                return Iterators.transform(authorizables, new Function<Authorizable, Authorizable>() {
                    @Nullable
                    @Override
                    public Authorizable apply(@Nullable Authorizable authorizable) {
                        return AuthorizableDelegator.wrap(sessionDelegate, authorizable);
                    }
                });
            }
        });
    }

    @Override
    public boolean isDeclaredMember(final Authorizable authorizable) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return groupDelegate.isDeclaredMember(authorizable);
            }
        });
    }

    @Override
    public boolean isMember(final Authorizable authorizable) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return groupDelegate.isMember(authorizable);
            }
        });
    }

    @Override
    public boolean addMember(final Authorizable authorizable) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return groupDelegate.addMember(authorizable);
            }
        });
    }

    @Override
    public boolean removeMember(final Authorizable authorizable) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return groupDelegate.removeMember(authorizable);
            }
        });
    }

    @Override
    public String getID() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                return groupDelegate.getID();
            }
        });
    }

    @Override
    public boolean isGroup() {
        return sessionDelegate.safePerform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() {
                return groupDelegate.isGroup();
            }
        });
    }

    @Override
    public Principal getPrincipal() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Principal>() {
            @Override
            public Principal perform() throws RepositoryException {
                return groupDelegate.getPrincipal();
            }
        });
    }

    @Override
    public Iterator<Group> declaredMemberOf() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<Group>>() {
            @Override
            public Iterator<Group> perform() throws RepositoryException {
                Iterator<Group> groups = groupDelegate.declaredMemberOf();
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
                Iterator<Group> groups = groupDelegate.memberOf();
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
                groupDelegate.remove();
                return null;
            }
        });
    }

    @Override
    public Iterator<String> getPropertyNames() throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<String>>() {
            @Override
            public Iterator<String> perform() throws RepositoryException {
                return groupDelegate.getPropertyNames();
            }
        });
    }

    @Override
    public Iterator<String> getPropertyNames(final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Iterator<String>>() {
            @Override
            public Iterator<String> perform() throws RepositoryException {
                return groupDelegate.getPropertyNames(relPath);
            }
        });
    }

    @Override
    public boolean hasProperty(final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return groupDelegate.hasProperty(relPath);
            }
        });
    }

    @Override
    public void setProperty(final String relPath, final Value value) throws RepositoryException {
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                groupDelegate.setProperty(relPath, value);
                return null;
            }
        });
    }

    @Override
    public void setProperty(final String relPath, final Value[] value) throws RepositoryException {
        sessionDelegate.perform(new SessionOperation<Void>() {
            @Override
            public Void perform() throws RepositoryException {
                groupDelegate.setProperty(relPath, value);
                return null;
            }
        });
    }

    @Override
    public Value[] getProperty(final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Value[]>() {
            @Override
            public Value[] perform() throws RepositoryException {
                return groupDelegate.getProperty(relPath);
            }
        });
    }

    @Override
    public boolean removeProperty(final String relPath) throws RepositoryException {
        return sessionDelegate.perform(new SessionOperation<Boolean>() {
            @Override
            public Boolean perform() throws RepositoryException {
                return groupDelegate.removeProperty(relPath);
            }
        });
    }

    @Override
    public String getPath() throws UnsupportedRepositoryOperationException, RepositoryException {
        return sessionDelegate.perform(new SessionOperation<String>() {
            @Override
            public String perform() throws RepositoryException {
                return groupDelegate.getPath();
            }
        });
    }
}
