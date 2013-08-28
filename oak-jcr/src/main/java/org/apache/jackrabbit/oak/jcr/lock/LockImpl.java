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

import org.apache.jackrabbit.oak.jcr.NodeImpl;
import org.apache.jackrabbit.oak.jcr.SessionContext;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.operation.NodeOperation;
import org.apache.jackrabbit.oak.jcr.operation.SessionOperation;

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
        return safePerform(new NodeOperation<Boolean>(delegate) {
            @Override
            public Boolean perform() throws RepositoryException {
                return node.holdsLock(false);
            }
        });
    }


    @Override
    public String getLockToken() {
        return null;
    }

    @Override
    public long getSecondsRemaining() {
        return Long.MAX_VALUE;
    }

    @Override
    public boolean isSessionScoped() {
        return false;
    }

    @Override
    public boolean isLockOwningSession() {
        return true;
    }

    @Override
    public void refresh() {
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
