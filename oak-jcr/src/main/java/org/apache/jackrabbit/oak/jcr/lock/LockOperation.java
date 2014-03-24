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

    private final NodeDelegate node;

    private final String path;

    protected LockOperation(SessionDelegate session, NodeDelegate node, String name) {
        super(name);
        this.session = session;
        this.path = null;
        this.node = node;
    }

    protected LockOperation(SessionContext context, String absPath, String name)
            throws PathNotFoundException {
        super(name);
        this.session = context.getSessionDelegate();
        this.path = context.getOakPathOrThrowNotFound(absPath);
        this.node = null;
    }

    @Override
    public boolean isRefresh() {
        return true;
    }

    @Override
    public T perform() throws RepositoryException {
        session.refresh(true);
        if (node != null) {
            return perform(node);
        } else {
            NodeDelegate node = session.getNode(path);
            if (node != null) {
                return perform(node);
            } else {
                throw new PathNotFoundException(
                        "Node " + path + " not found");
            }
        }
    }

    protected abstract T perform(NodeDelegate node)
            throws RepositoryException;

}