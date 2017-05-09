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

package org.apache.jackrabbit.oak.jcr.delegate;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.jcr.InvalidItemStateException;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.Tree.Status;

/**
 * Abstract base class for {@link NodeDelegate} and {@link PropertyDelegate}
 */
public abstract class ItemDelegate {

    protected final SessionDelegate sessionDelegate;

    /**
     * The session update count. Used to avoid the overhead of extra
     * {@link #exists()} calls every time this item is accessed.
     *
     * @see #checkAlive()
     */
    private long updateCount;

    ItemDelegate(SessionDelegate sessionDelegate) {
        this.sessionDelegate = checkNotNull(sessionDelegate);
        this.updateCount  = sessionDelegate.getUpdateCount();
    }

    /**
     * Checks whether the session has changed since this delegate instance
     * was last accessed, thus triggering an {@link #update() update} of the
     * internal state of this delegate.
     *
     * @return {@code true} if the session was recently updated,
     *         {@code false} if not
     */
    protected boolean checkUpdate() {
        long sessionCount = sessionDelegate.getUpdateCount();
        if (updateCount != sessionCount) {
            updateCount = sessionCount;
            update();
            return true;
        } else {
            return false;
        }
    }

    /**
     * Called by {@link #checkUpdate()} to update the internal state of this
     * delegate.
     */
    protected void update() {
    }

    /**
     * Performs a sanity check on this item and the associated session.
     *
     * @throws RepositoryException if this item has been rendered invalid
     *                             for some reason or the associated session
     *                             has been logged out
     */
    public synchronized void checkAlive() throws RepositoryException {
        sessionDelegate.checkAlive();
        if (checkUpdate() && !exists()) {
            throw new InvalidItemStateException(
                    "This item [" + getPath() + "] does not exist anymore");
        }
    }

    /**
     * Get the name of this item
     * @return oak name of this item
     */
    @Nonnull
    public abstract String getName();

    /**
     * Get the path of this item
     * @return oak path of this item
     */
    @Nonnull
    public abstract String getPath();

    /**
     * Get the parent of this item or {@code null}.
     * @return  parent of this item or {@code null} for root or if the parent
     * is not accessible.
     */
    @CheckForNull
    public abstract NodeDelegate getParent();

    /**
     * Get the status of this item.
     * @return  {@link Status} of this item or {@code null} if not available.
     */
    @CheckForNull
    public abstract Status getStatus();

    public abstract boolean isProtected() throws InvalidItemStateException;

    /**
     * Determine whether the underlying item exists
     * @return  {@code true} the underlying tree exists, {@code false} otherwise.
     */
    public abstract boolean exists();

    /**
     * Removes this item.
     *
     * @return {@code true} if this item was removed;
     *         or {@code false} if this is the root node that can't be removed
     */
    public abstract boolean remove() throws InvalidItemStateException;

    /**
     * Save the subtree rooted at this item.
     *
     * @throws RepositoryException
     */
    public void save() throws RepositoryException {
        sessionDelegate.save(getPath());
    }
}
