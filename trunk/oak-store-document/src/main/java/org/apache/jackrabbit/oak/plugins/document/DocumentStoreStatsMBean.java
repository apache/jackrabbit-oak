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

import javax.management.openmbean.CompositeData;

import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface DocumentStoreStatsMBean {

    String TYPE = "DocumentStoreStats";

    long getNodesFindCount();

    long getNodesFindQueryCount();

    long getNodesFindMissingCount();

    long getNodesReadByQueryCount();

    long getNodesCreateCount();

    long getNodesUpdateCount();

    long getNodesRemoveCount();

    long getJournalCreateCount();

    long getJournalReadCount();

    CompositeData getFindCachedNodesHistory();

    CompositeData getFindSplitNodesHistory();

    CompositeData getFindNodesFromPrimaryHistory();

    CompositeData getFindNodesFromSlaveHistory();

    CompositeData getFindNodesMissingHistory();

    CompositeData getQueryNodesFromSlaveHistory();

    CompositeData getQueryNodesFromPrimaryHistory();

    CompositeData getQueryNodesLockHistory();

    CompositeData getQueryJournalHistory();

    CompositeData getCreateJournalHistory();

    CompositeData getCreateNodesHistory();

    CompositeData getUpdateNodesHistory();

    CompositeData getUpdateNodesRetryHistory();

    CompositeData getUpdateNodesFailureHistory();

    CompositeData getRemoveNodesHistory();
}
