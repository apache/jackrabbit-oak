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

package org.apache.jackrabbit.oak.jcr;

import org.apache.jackrabbit.oak.api.Tree.Status;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;

/**
 * Abstract base class for {@link NodeDelegate} and {@link PropertyDelegate}
 */
public abstract class ItemDelegate {

    protected final SessionDelegate sessionDelegate;

    protected ItemDelegate(SessionDelegate sessionDelegate) {
        assert sessionDelegate != null;

        this.sessionDelegate = sessionDelegate;
    }

    /**
     * Get the name of this item
     * @return oak name of this item
     */
    @Nonnull
    public abstract String getName() throws InvalidItemStateException;

    /**
     * Get the path of this item
     * @return oak path of this item
     */
    @Nonnull
    public abstract String getPath() throws InvalidItemStateException;

    /**
     * Get the parent of this item or {@code null}.
     * @return  parent of this item or {@code null} for root or if the parent
     * is not accessible.
     */
    @CheckForNull
    public abstract NodeDelegate getParent() throws InvalidItemStateException;

    /**
     * Determine whether this item is stale
     * @return  {@code true} iff stale
     */
    public abstract boolean isStale();

    /**
     * Get the status of this item
     * @return  {@link Status} of this item
     */
    @Nonnull
    public abstract Status getStatus() throws InvalidItemStateException;

    /**
     * Get the session delegate with which this item is associated
     * @return  {@link SessionDelegate} to which this item belongs
     */
    @Nonnull
    public SessionDelegate getSessionDelegate() {
        return sessionDelegate;
    }
}
