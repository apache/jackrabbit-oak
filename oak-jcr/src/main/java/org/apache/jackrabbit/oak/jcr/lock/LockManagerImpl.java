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
package org.apache.jackrabbit.oak.jcr.lock;

import static java.lang.Boolean.TRUE;
import static org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl.RELAXED_LOCKING;

import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.lock.Lock;
import javax.jcr.lock.LockException;
import javax.jcr.lock.LockManager;

import org.apache.jackrabbit.api.security.user.Authorizable;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.oak.api.Tree.Status;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

/**
 * Simple lock manager implementation that just keeps track of a set of lock
 * tokens and delegates all locking operations back to the {@link Session}
 * and {@link Node} implementations.
 */
public class LockManagerImpl implements LockManager {

    private final SessionContext sessionContext;

    private final SessionDelegate delegate;

    public LockManagerImpl(SessionContext sessionContext) {
        this.sessionContext = sessionContext;
        this.delegate = sessionContext.getSessionDelegate();
    }

    @Nonnull
    @Override
    public String[] getLockTokens() throws RepositoryException {
        return delegate.perform(new SessionOperation<String[]>("getLockTokens") {
            @Nonnull
            @Override
            public String[] perform() {
                Set<String> tokens = sessionContext.getOpenScopedLocks();
                return tokens.toArray(new String[tokens.size()]);
            }
        });
    }

    @Override
    public void addLockToken(final String lockToken)
            throws RepositoryException {
        try {
            delegate.performVoid(new LockOperation<Void>(sessionContext, lockToken, "addLockToken") {
                @Override
                protected void performVoid(@Nonnull NodeDelegate node) throws LockException {
                    if (node.holdsLock(false)) { // TODO: check ownership?
                        String token = node.getPath();
                        sessionContext.getOpenScopedLocks().add(token);
                    } else {
                        throw new LockException("Invalid lock token: " + lockToken);
                    }
                }
            });
        } catch (IllegalArgumentException e) { // TODO: better exception
            throw new LockException("Invalid lock token: " + lockToken);
        }
    }

    @Override
    public void removeLockToken(final String lockToken) throws RepositoryException {
        if (!delegate.perform(new SessionOperation<Boolean>("removeLockToken") {
            @Nonnull
            @Override
            public Boolean perform() {
                // TODO: name mapping?
                return sessionContext.getOpenScopedLocks().remove(lockToken);
            }
        })) {
            throw new LockException("Lock token " + lockToken + " is not held by this session");
        }
    }

    @Override
    public boolean isLocked(String absPath) throws RepositoryException {
        return delegate.perform(new LockOperation<Boolean>(sessionContext, absPath, "isLocked") {
            @Nonnull
            @Override
            protected Boolean perform(@Nonnull NodeDelegate node) {
                return node.isLocked();
            }
        });
    }

    @Override
    public boolean holdsLock(String absPath) throws RepositoryException {
        return delegate.perform(new LockOperation<Boolean>(sessionContext, absPath, "holdsLock") {
            @Nonnull
            @Override
            protected Boolean perform(@Nonnull NodeDelegate node) {
                return node.holdsLock(false);
            }
        });
    }

    @Nonnull
    @Override
    public Lock getLock(final String absPath) throws RepositoryException {
        NodeDelegate lock = delegate.perform(new LockOperation<NodeDelegate>(sessionContext, absPath, "getLock") {
            @Nonnull
            @Override
            protected NodeDelegate perform(@Nonnull NodeDelegate node) throws LockException {
                NodeDelegate lock = node.getLock();
                if (lock == null) {
                    throw new LockException("Node " + absPath + " is not locked");
                } else {
                    return lock;
                }
            }
        });

        return new LockImpl(sessionContext, lock);
    }

    @Nonnull
    @Override
    public Lock lock(String absPath, final boolean isDeep, final boolean isSessionScoped,
                     long timeoutHint, String ownerInfo) throws RepositoryException {
        return new LockImpl(sessionContext, delegate.perform(new LockOperation<NodeDelegate>(sessionContext, absPath, "lock") {
            @Nonnull
            @Override
            protected NodeDelegate perform(@Nonnull NodeDelegate node) throws RepositoryException {
                if (node.getStatus() != Status.UNCHANGED) {
                    throw new InvalidItemStateException(
                            "Unable to lock a node with pending changes");
                }
                node.lock(isDeep);
                String path = node.getPath();
                if (isSessionScoped) {
                    sessionContext.getSessionScopedLocks().add(path);
                } else {
                    sessionContext.getOpenScopedLocks().add(path);
                }
                session.refresh(true);
                return node;
            }
        }));
    }

    @Override
    public void unlock(String absPath) throws RepositoryException {
        delegate.performVoid(new LockOperation<Void>(sessionContext, absPath, "unlock") {
            @Override
            protected void performVoid(@Nonnull NodeDelegate node)
                    throws RepositoryException {
                String path = node.getPath();
                if (canUnlock(node)) {
                    node.unlock();
                    sessionContext.getSessionScopedLocks().remove(path);
                    sessionContext.getOpenScopedLocks().remove(path);
                    session.refresh(true);
                } else {
                    throw new LockException("Not an owner of the lock " + path);
                }
            }
        });
    }
    
    /**
     * Verifies if the current <tt>sessionContext</tt> can unlock the specified <tt>node</tt>
     * 
     * @param node the node state to check
     * 
     * @return true if the current <tt>sessionContext</tt> can unlock the specified <tt>node</tt>
     */
    public boolean canUnlock(NodeDelegate node) {
        String path = node.getPath();
        if (sessionContext.getSessionScopedLocks().contains(path)
                || sessionContext.getOpenScopedLocks().contains(path)) {
            return true;
        } else if (sessionContext.getAttributes().get(RELAXED_LOCKING) == TRUE) {
            String user = sessionContext.getSessionDelegate().getAuthInfo().getUserID();
            return node.isLockOwner(user) || isAdmin(sessionContext, user);
        } else {
            return false;
        }
    }

    private boolean isAdmin(SessionContext sessionContext, String user) {
        try {
            Authorizable a = sessionContext.getUserManager().getAuthorizable(
                    user);
            if (a != null && !a.isGroup()) {
                return ((User) a).isAdmin();
            }
        } catch (RepositoryException e) {
            // ?
        }
        return false;
    }
}