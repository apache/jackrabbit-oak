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

package org.apache.jackrabbit.oak.plugins.document;

import org.apache.jackrabbit.oak.commons.jmx.Name;

@SuppressWarnings("UnusedDeclaration")
public interface DocumentNodeStoreMBean {
    String TYPE = "DocumentNodeStore";

    String getRevisionComparatorState();

    String getHead();

    int getClusterId();

    int getUnmergedBranchCount();

    String[] getInactiveClusterNodes();

    String[] getActiveClusterNodes();

    String[] getLastKnownRevisions();

    String formatRevision(@Name("revision") String rev, @Name("UTC")boolean utc);

    /**
     * @return the estimated time difference in milliseconds between
     * the local instance and the (typically common, shared) document server system.
     * The value can be zero if the times are estimated to be equal,
     * positive when the local instance is ahead of the remote server
     * and negative when the local instance is behind the remote server. An invocation is not cached
     * and typically requires a round-trip to the server (but that is not a requirement).
     * @throws UnsupportedOperationException if this DocumentStore does not support this method
     */
    long determineServerTimeDifferenceMillis();
}
