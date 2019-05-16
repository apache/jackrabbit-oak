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

package org.apache.jackrabbit.oak.plugins.document.rdb;

import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QueryCondition;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;

/**
 * RDB specific version of MissingLastRevSeeker.
 */
public class RDBMissingLastRevSeeker extends MissingLastRevSeeker {
    private final RDBDocumentStore store;

    public RDBMissingLastRevSeeker(RDBDocumentStore store, Clock clock) {
        super(store, clock);
        this.store = store;
    }

    @Override
    @NotNull
    public Iterable<NodeDocument> getCandidates(final long startTime) {
        List<QueryCondition> conditions = Collections
                .singletonList(new QueryCondition(NodeDocument.MODIFIED_IN_SECS, ">=", NodeDocument.getModifiedInSecs(startTime)));
        return store.queryAsIterable(Collection.NODES, null, null, RDBDocumentStore.EMPTY_KEY_PATTERN, conditions,
                Integer.MAX_VALUE, null);
    }
}
