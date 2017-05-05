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

package org.apache.jackrabbit.oak.plugins.index;

import javax.annotation.CheckForNull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.jmx.IndexStatsMBean;

import static com.google.common.base.Preconditions.checkNotNull;

public class AsyncIndexInfo {
    private final String name;
    private final long lastIndexedTo;
    private final long leaseExpiryTime;
    private final boolean running;
    private final IndexStatsMBean statsMBean;

    public AsyncIndexInfo(String name, long lastIndexedTo, long leaseExpiryTime, boolean running, @Nullable IndexStatsMBean statsMBean) {
        this.name = checkNotNull(name);
        this.lastIndexedTo = lastIndexedTo;
        this.leaseExpiryTime = leaseExpiryTime;
        this.running = running;
        this.statsMBean = statsMBean;
    }

    public String getName() {
        return name;
    }

    /**
     * Time in millis upto which the repository state is indexed via
     * this indexer
     */
    public long getLastIndexedTo() {
        return lastIndexedTo;
    }

    /**
     * Time in millis at which the current help lease would expire if
     * indexing is in progress. If indexing is not in progress then its
     * value would be -1
     */
    public long getLeaseExpiryTime() {
        return leaseExpiryTime;
    }

    /**
     * Returns true if the async indexer is currently active
     */
    public boolean isRunning() {
        return running;
    }

    /**
     * IndexStatsMBean for current indexer. The MBean would be
     * returning valid values only for that cluster node where
     * the async indexer is active. For other cluster nodes
     * the values may not reflect the current state
     */
    @CheckForNull
    public IndexStatsMBean getStatsMBean() {
        return statsMBean;
    }

    @Override
    public String toString() {
        return String.format("%s : lastIndexedTo :%tc, leaseExpiryTime :%tc, running :%s",
                name, lastIndexedTo, leaseExpiryTime, running);
    }
}
