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

import org.apache.jackrabbit.oak.jcr.delegate.SessionDelegate;
import org.jetbrains.annotations.NotNull;

public abstract class UserManagerOperation<T> extends SessionOperation<T> {
    private final SessionDelegate delegate;

    protected UserManagerOperation(@NotNull SessionDelegate delegate, @NotNull String name) {
        this(delegate, name, false);
    }

    protected UserManagerOperation(@NotNull SessionDelegate delegate, @NotNull String name, boolean update) {
        super(name, update);
        this.delegate = delegate;
    }

    @Override
    public void checkPreconditions() throws RepositoryException {
        delegate.checkAlive();
    }
}
