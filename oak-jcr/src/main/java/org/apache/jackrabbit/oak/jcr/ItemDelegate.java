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

/**
 * Abstract base class for {@link NodeDelegate} and {@link PropertyDelegate}
 */
public abstract class ItemDelegate {

    protected final SessionDelegate sessionDelegate;

    protected ItemDelegate(SessionDelegate sessionDelegate) {
        this.sessionDelegate = sessionDelegate;
    }

    /**
     * Get the name of this item
     * @return oak name of this item
     */
    abstract String getName();

    /**
     * Get the path of this item
     * @return oak path of this item
     */
    abstract String getPath();

    /**
     * Get the parent of this item
     * @return  parent of this item or {@code null} for root
     */
    abstract NodeDelegate getParent();

    /**
     * Determine whether this item is stale
     * @return  {@code true} iff stale
     */
    abstract boolean isStale();

    /**
     * Get the status of this item
     * @return  {@link Status} of this item
     */
    abstract Status getStatus();

    /**
     * Get the session delegate with which this item is associated
     * @return  {@link SessionDelegate} to which this item belongs
     */
    abstract SessionDelegate getSessionDelegate();
}
