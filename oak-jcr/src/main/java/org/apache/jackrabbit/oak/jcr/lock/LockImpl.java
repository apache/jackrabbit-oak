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

import static com.google.common.base.Preconditions.checkNotNull;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.lock.Lock;
import javax.jcr.lock.LockException;

import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.session.NodeImpl;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.session.operation.NodeOperation;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class LockImpl implements Lock {

    private final SessionContext context;

    private final NodeDelegate delegate;

    public LockImpl(@NotNull SessionContext context, @NotNull NodeDelegate delegate) {
        this.context = checkNotNull(context);
        this.delegate = checkNotNull(delegate);
    }

    @Override
    public Node getNode() {
        try {
            return NodeImpl.createNode(delegate, context);
        } catch (RepositoryException e) {
            throw new RuntimeException("Unable to access the lock node", e);
        }
    }

    @Override
    public String getLockOwner() {
        return savePerformNullable(new NodeOperation<String>(delegate, "getLockOwner") {
            @Override
            public String performNullable() {
                return node.getLockOwner();
            }
        });
    }

    @Override
    public boolean isDeep() {
        return getSessionDelegate().safePerform(new NodeOperation<Boolean>(delegate, "isDeep") {
            @NotNull
            @Override
            public Boolean perform() {
                return node.holdsLock(true);
            }
        });
    }

    @Override
    public boolean isLive() {
        return context.getSession().isLive() && getSessionDelegate().safePerform(
                new NodeOperation<Boolean>(delegate, "isLive") {
                    @NotNull
                    @Override
                    public Boolean perform() {
                        return node.holdsLock(false);
                    }
                });
    }


    @Override
    public String getLockToken() {
        return savePerformNullable(new NodeOperation<String>(delegate, "getLockToken") {
            @Override
            public String performNullable() {
                String token = node.getPath();
                if (context.getOpenScopedLocks().contains(token)) {
                    return token;
                } else if (context.getSessionScopedLocks().contains(token)) {
                    // Prevent session-scoped locks from exposing their
                    // tokens to the session that holds the lock. However,
                    // another session of the lock owner will be able to
                    // acquire the lock token and thus release the lock.
                    return null;
                } else if (node.isLockOwner(
                        context.getSessionDelegate().getAuthInfo().getUserID())) {
                    // The JCR spec allows the implementation to return the
                    // lock token even when the current session isn't already
                    // holding it. We use this feature to allow all sessions
                    // of the user who owns the lock to access its token.
                    return token;
                } else {
                    return null;
                }
            }
        });
    }

    @Override
    public long getSecondsRemaining() {
        if (isLive()) {
            return Long.MAX_VALUE;
        } else {
            return -1;
        }
    }

    @Override
    public boolean isSessionScoped() {
        return getSessionDelegate().safePerform(new NodeOperation<Boolean>(delegate, "isSessionScoped") {
            @NotNull
            @Override
            public Boolean perform() {
                String path = node.getPath();
                return context.getSessionScopedLocks().contains(path);
            }
        });
    }

    @Override
    public boolean isLockOwningSession() {
        return getSessionDelegate().safePerform(new NodeOperation<Boolean>(delegate, "isLockOwningSessions") {
            @NotNull
            @Override
            public Boolean perform() {
                String path = node.getPath();
                return context.getSessionScopedLocks().contains(path)
                        || context.getOpenScopedLocks().contains(path);
            }
        });
    }

    @Override
    public void refresh() throws LockException {
        if (!isLive()) {
            throw new LockException("This lock is not alive");
        }
    }

    //-----------------------------------------------------------< private >--

    private SessionDelegate getSessionDelegate() {
        return context.getSessionDelegate();
    }

    @Nullable
    private <U> U savePerformNullable(@NotNull SessionOperation<U> op) {
        try {
            return context.getSessionDelegate().performNullable(op);
        } catch (RepositoryException e) {
            throw new RuntimeException("Unexpected exception thrown by operation " + op, e);
        }
    }
}
