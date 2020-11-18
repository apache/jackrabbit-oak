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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.MissingLastRevSeeker;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QueryCondition;
import org.apache.jackrabbit.oak.plugins.document.util.SystemPropertySupplier;
import org.apache.jackrabbit.oak.stats.Clock;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RDB specific version of MissingLastRevSeeker.
 */
public class RDBMissingLastRevSeeker extends MissingLastRevSeeker {

    private static final Logger LOG = LoggerFactory.getLogger(RDBMissingLastRevSeeker.class);

    // 1: seek using historical, paging mode
    // 2: use custom single query directly using RDBDocumentStore API
    private static final int DEFAULTMODE = 2;

    private static final int MODE = SystemPropertySupplier.create(RDBMissingLastRevSeeker.class.getName() + ".MODE", DEFAULTMODE)
            .loggingTo(LOG).validateWith(value -> (value == 1 || value == 2)).formatSetMessage((name, value) -> {
                return String.format("Strategy for %s set to %s (via system property %s)", RDBMissingLastRevSeeker.class.getName(),
                        name, value);
            }).get();

    private final RDBDocumentStore store;

    public RDBMissingLastRevSeeker(RDBDocumentStore store, Clock clock) {
        super(store, clock);
        this.store = store;
    }

    @Override
    @NotNull
    public Iterable<NodeDocument> getCandidates(final long startTime) {
        LOG.debug("Running getCandidates() in mode " + MODE);
        if (MODE == 1) {
            return super.getCandidates(startTime);
        } else {
            List<QueryCondition> conditions = new ArrayList<>();
            conditions.add(new QueryCondition(NodeDocument.MODIFIED_IN_SECS, ">=", NodeDocument.getModifiedInSecs(startTime)));
            conditions.add(new QueryCondition(NodeDocument.SD_TYPE, "is null"));
            return store.queryAsIterable(Collection.NODES, null, null, RDBDocumentStore.EMPTY_KEY_PATTERN, conditions,
                    Integer.MAX_VALUE, null);
        }
    }
}
