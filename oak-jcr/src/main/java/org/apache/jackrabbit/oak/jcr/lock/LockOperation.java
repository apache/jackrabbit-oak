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

import javax.annotation.Nonnull;
import javax.jcr.PathNotFoundException;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.jcr.session.SessionContext;
import org.apache.jackrabbit.oak.jcr.delegate.NodeDelegate;
import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.session.operation.SessionOperation;

/**
 * Abstract base class for locking operations.
 *
 * @param <T> return type of the {@link #perform()} method
 */
public abstract class LockOperation<T> extends SessionOperation<T> {

    protected final SessionDelegate session;
    private final String path;

    protected LockOperation(SessionContext context, String absPath, String name)
            throws PathNotFoundException {
        super(name);
        this.session = context.getSessionDelegate();
        this.path = context.getOakPathOrThrowNotFound(absPath);
    }

    @Override
    public boolean isRefresh() {
        return true;
    }

    @Nonnull
    @Override
    public T perform() throws RepositoryException {
        session.refresh(true);

        NodeDelegate node = session.getNode(path);
        if (node != null) {
            return perform(node);
        } else {
            throw new PathNotFoundException("Node " + path + " not found");
        }
    }

    @Override
    public void performVoid() throws RepositoryException {
        session.refresh(true);

        NodeDelegate node = session.getNode(path);
        if (node != null) {
            performVoid(node);
        } else {
            throw new PathNotFoundException("Node " + path + " not found");
        }
    }

    @Nonnull
    protected T perform(@Nonnull NodeDelegate node) throws RepositoryException {
        throw new UnsupportedOperationException();
    }

    protected void performVoid(@Nonnull NodeDelegate node) throws RepositoryException {
        throw new UnsupportedOperationException();
    }

}