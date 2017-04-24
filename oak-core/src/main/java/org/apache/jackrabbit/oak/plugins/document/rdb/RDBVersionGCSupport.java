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
package org.apache.jackrabbit.oak.plugins.document.rdb;

import static com.google.common.collect.Iterables.filter;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.DocumentStoreException;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import org.apache.jackrabbit.oak.plugins.document.RevisionVector;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QueryCondition;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;

/**
 * RDB specific version of {@link VersionGCSupport} which uses an extended query
 * interface to fetch required {@link NodeDocument}s.
 */
public class RDBVersionGCSupport extends VersionGCSupport {

    private static final Logger LOG = LoggerFactory.getLogger(RDBVersionGCSupport.class);

    private RDBDocumentStore store;

    public RDBVersionGCSupport(RDBDocumentStore store) {
        super(store);
        this.store = store;
    }

    @Override
    public Iterable<NodeDocument> getPossiblyDeletedDocs(final long fromModified, final long toModified) {
        List<QueryCondition> conditions = new ArrayList<QueryCondition>();
        conditions.add(new QueryCondition(NodeDocument.DELETED_ONCE, "=", 1));
        conditions.add(new QueryCondition(NodeDocument.MODIFIED_IN_SECS, "<", NodeDocument.getModifiedInSecs(toModified)));
        conditions.add(new QueryCondition(NodeDocument.MODIFIED_IN_SECS, ">=", NodeDocument.getModifiedInSecs(fromModified)));
        return store.queryAsIterable(Collection.NODES, null, null, RDBDocumentStore.EMPTY_KEY_PATTERN, conditions, Integer.MAX_VALUE, null);
    }

    @Override
    protected Iterable<NodeDocument> identifyGarbage(final Set<SplitDocType> gcTypes,
                                                     final RevisionVector sweepRevs,
                                                     final long oldestRevTimeStamp) {
        List<QueryCondition> conditions = Collections.emptyList();
        // absent support for SDTYPE as indexed property: exclude those
        // documents from the query which definitively aren't split documents
        List<String> excludeKeyPatterns = Arrays.asList("_:/%", "__:/%", "___:/%");
        Iterable<NodeDocument> it = store.queryAsIterable(Collection.NODES, null, null, excludeKeyPatterns, conditions,
                Integer.MAX_VALUE, null);
        return CloseableIterable.wrap(filter(it, new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument doc) {
                return gcTypes.contains(doc.getSplitDocType())
                        && doc.hasAllRevisionLessThan(oldestRevTimeStamp)
                        && !isDefaultNoBranchSplitNewerThan(doc, sweepRevs);
            }
        }), (Closeable) it);
    }

    @Override
    public long getOldestDeletedOnceTimestamp(Clock clock, long precisionMs) {
        long modifiedMs = Long.MIN_VALUE;

        LOG.debug("getOldestDeletedOnceTimestamp() <- start");
        try {
            modifiedMs = store.getMinValue(Collection.NODES, NodeDocument.MODIFIED_IN_SECS, null, null,
                    RDBDocumentStore.EMPTY_KEY_PATTERN,
                    Collections.singletonList(new QueryCondition(NodeDocument.DELETED_ONCE, "=", 1)));
        } catch (DocumentStoreException ex) {
            LOG.debug("getMinValue(MODIFIED)", ex);
        }

        if (modifiedMs > 0) {
            LOG.debug("getOldestDeletedOnceTimestamp() -> {}", Utils.timestampToString(modifiedMs));
            return modifiedMs;
        } else {
            LOG.debug("getOldestDeletedOnceTimestamp() -> none found, return current time");
            return clock.getTime();
        }
    }

    @Override
    public long getDeletedOnceCount() {
        return store.queryCount(Collection.NODES, null, null, RDBDocumentStore.EMPTY_KEY_PATTERN,
                Collections.singletonList(new QueryCondition(NodeDocument.DELETED_ONCE, "=", 1)));
    }
}