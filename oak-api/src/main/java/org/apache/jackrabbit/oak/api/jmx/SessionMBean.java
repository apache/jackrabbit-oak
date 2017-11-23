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

package org.apache.jackrabbit.oak.api.jmx;

import org.osgi.annotation.versioning.ProviderType;
import org.apache.jackrabbit.oak.api.AuthInfo;

/**
 * MBean providing basic {@code Session} information and statistics.
 */
@ProviderType
public interface SessionMBean {
    String TYPE = "SessionStatistics";

    /**
     * @return stack trace from where the session was acquired.
     */
    String getInitStackTrace();

    /**
     * @return {@code AuthInfo} for the user associated with the session.
     */
    AuthInfo getAuthInfo();

    /**
     * @return time stamp from when the session was acquired.
     */
    String getLoginTimeStamp();

    /**
     * @return time stamp from the last read access
     */
    String getLastReadAccess();

    /**
     * @return number of read accesses
     */
    long getReadCount();

    /**
     * @return read operations per time
     */
    double getReadRate();

    /**
     * @return time stamp from the last write access
     */
    String getLastWriteAccess();

    /**
     * @return number of write accesses
     */
    long getWriteCount();

    /**
     * @return write operations per time
     */
    double getWriteRate();

    /**
     * @return time stamp from the last refresh
     */
    String getLastRefresh();

    /**
     * @return description of the refresh strategy
     */
    String getRefreshStrategy();

    /**
     * @return {@code true} iff the session will be refreshed on next access.
     */
    boolean getRefreshPending();

    /**
     * @return number of refresh operations
     */
    long getRefreshCount();

    /**
     * @return refresh operations per time
     */
    double getRefreshRate();

    /**
     * @return time stamp from the last save
     */
    String getLastSave();

    /**
     * @return number of save operations
     */
    long getSaveCount();

    /**
     * @return save operations per time
     */
    double getSaveRate();

    /**
     * @return attributes associated with the session
     */
    String[] getSessionAttributes();

    /**
     * @return  stack trace of the last exception that occurred during a save operation
     */
    String getLastFailedSave();

    /**
     * Refresh this session.
     * <em>Warning</em>: this operation might be disruptive to the owner of this session
     */
    void refresh();
}
