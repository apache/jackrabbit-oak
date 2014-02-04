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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.lock.Lock;
import javax.jcr.lock.LockException;

import org.apache.jackrabbit.oak.jcr.session.NodeImpl;
import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.session.operation.NodeOperation;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

public final class LockImpl implements Lock {

    private final SessionContext context;

    private final NodeDelegate delegate;

    public LockImpl(
            @Nonnull SessionContext context, @Nonnull NodeDelegate delegate) {
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
        return safePerform(new NodeOperation<String>(delegate) {
            @Override
            public String perform() throws RepositoryException {
                return node.getLockOwner();
            }
        });
    }

    @Override
    public boolean isDeep() {
        return safePerform(new NodeOperation<Boolean>(delegate) {
            @Override
            public Boolean perform() throws RepositoryException {
                return node.holdsLock(true);
            }
        });
    }

    @Override
    public boolean isLive() {
        return context.getSession().isLive() && safePerform(
                new NodeOperation<Boolean>(delegate) {
                    @Override
                    public Boolean perform() throws RepositoryException {
                        return node.holdsLock(false);
                    }
                });
    }


    @Override
    public String getLockToken() {
        return safePerform(new NodeOperation<String>(delegate) {
            @Override
            public String perform() throws RepositoryException {
                String token = node.getPath();
                if (context.getOpenScopedLocks().contains(token)) {
                    return token;
                } else if (context.getSessionScopedLocks().contains(token)) {
                    // Prevent session-scoped locks from exposing their
                    // tokens to the session that holds the lock. However,
                    // another session of the lock owner will be able to
                    // acquire the lock token and thus release the lock.
                    return null;
                }

                String owner =
                        context.getSessionDelegate().getAuthInfo().getUserID();
                if (owner == null) {
                    owner = "";
                }
                if (owner.equals(node.getLockOwner())) {
                    return token;
                }

                return null;
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
        return safePerform(new NodeOperation<Boolean>(delegate) {
            @Override
            public Boolean perform() throws RepositoryException {
                String path = node.getPath();
                return context.getSessionScopedLocks().contains(path);
            }
        });
    }

    @Override
    public boolean isLockOwningSession() {
        return safePerform(new NodeOperation<Boolean>(delegate) {
            @Override
            public Boolean perform() throws RepositoryException {
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

    /**
     * Perform the passed {@link SessionOperation} assuming it does not
     * throw a {@code RepositoryException}. If it does, wrap it into and
     * throw it as a {@code RuntimeException}.
     *
     * @param op operation to perform
     * @param <U> return type of the operation
     * @return the result of {@code op.perform()}
     */
    @CheckForNull
    private final <U> U safePerform(@Nonnull SessionOperation<U> op) {
        return context.getSessionDelegate().safePerform(op);
    }

}
