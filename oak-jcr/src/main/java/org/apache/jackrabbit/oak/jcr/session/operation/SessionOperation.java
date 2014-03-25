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
package org.apache.jackrabbit.oak.jcr.session.operation;

import javax.jcr.RepositoryException;

/**
 * A {@code SessionOperation} provides an execution context for executing session scoped operations.
 * @see org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate#perform(SessionOperation)
 */
public abstract class SessionOperation<T> {
    private final String name;
    private final boolean update;

    protected SessionOperation(String name, boolean update) {
        this.name = name;
        this.update = update;
    }

    protected SessionOperation(String name) {
        this(name, false);
    }

    /**
     * Returns {@code true} if this operation updates the the transient
     */
    public boolean isUpdate() {
        return update;
    }

    /**
     * Return {@code true} if this operation refreshed the transient space
     */
    public boolean isRefresh() {
        return false;
    }

    public boolean isSave() {
        return false;
    }

    public boolean isLogout() {
        return false;
    }

    public void checkPreconditions() throws RepositoryException {
    }

    public abstract T perform() throws RepositoryException;

    /**
     * Provide details about the operation being performed.
     * This default implementation just returns the
     * name passed to the constructor.
     */
    @Override
    public String toString() {
        return name;
    }

}
