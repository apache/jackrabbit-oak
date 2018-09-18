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

import java.io.Closeable;
import java.io.IOException;
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
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.UnsupportedIndexedPropertyException;
import org.apache.jackrabbit.oak.plugins.document.util.CloseableIterable;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.apache.jackrabbit.oak.stats.Clock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

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
        Iterable<NodeDocument> it1;
        Iterable<NodeDocument> it2;
        String name1, name2;

        // for schema 0 or 1 rows, we'll have to constrain the path
        List<String> excludeKeyPatterns = Arrays.asList("_:/%", "__:/%", "___:/%");

        try {
            List<Integer> gcTypeCodes = Lists.newArrayList();
            for (SplitDocType type : gcTypes) {
                gcTypeCodes.add(type.typeCode());
            }

            List<QueryCondition> conditions1 = new ArrayList<QueryCondition>();
            conditions1.add(new QueryCondition(NodeDocument.SD_TYPE, "in", gcTypeCodes));
            conditions1.add(new QueryCondition(NodeDocument.SD_MAX_REV_TIME_IN_SECS, "<=", NodeDocument.getModifiedInSecs(oldestRevTimeStamp)));
            conditions1.add(new QueryCondition(RDBDocumentStore.VERSIONPROP, ">=", 2));
            name1 = "version 2 query";
            it1 = store.queryAsIterable(Collection.NODES, null, null, Collections.emptyList(), conditions1,
                    Integer.MAX_VALUE, null);

            List<QueryCondition> conditions2 = new ArrayList<QueryCondition>();
            conditions2.add(new QueryCondition(RDBDocumentStore.VERSIONPROP, "null or <", 2));
            it2 = store.queryAsIterable(Collection.NODES, null, null, excludeKeyPatterns, conditions2,
                    Integer.MAX_VALUE, null);
            name2 = "version <2 fallback on " + excludeKeyPatterns;
        } catch (UnsupportedIndexedPropertyException ex) {
            // this will happen if we query a table that doesn't have the SD*
            // columns - create a new query without the constraint, and let the
            // Java code filter the results
            it1 = store.queryAsIterable(Collection.NODES, null, null, excludeKeyPatterns, Collections.emptyList(),
                    Integer.MAX_VALUE, null);
            it2 = Collections.emptySet();
            name1 = "version <2 fallback on " + excludeKeyPatterns;
            name2 = "";
        }

        final Iterable<NodeDocument> fit1 = it1;
        final Iterable<NodeDocument> fit2 = it2;

        Predicate<NodeDocument> pred =  new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument doc) {
                return gcTypes.contains(doc.getSplitDocType()) && doc.hasAllRevisionLessThan(oldestRevTimeStamp)
                        && !isDefaultNoBranchSplitNewerThan(doc, sweepRevs);
            }
        };

        final CountingPredicate<NodeDocument> cp1 = new CountingPredicate<NodeDocument>(name1, pred);
        final CountingPredicate<NodeDocument> cp2 = new CountingPredicate<NodeDocument>(name2, pred);

        return CloseableIterable.wrap(Iterables.concat(Iterables.filter(fit1, cp1), Iterables.filter(fit2, cp2)), new Closeable() {
            @Override
            public void close() throws IOException {
                Utils.closeIfCloseable(fit1);
                Utils.closeIfCloseable(fit2);
                if (LOG.isDebugEnabled()) {
                    String stats1 = cp1.getStats();
                    String stats2 = cp2.getStats();
                    String message = "";
                    if (!stats1.isEmpty()) {
                        message = stats1;
                    }
                    if (!stats2.isEmpty()) {
                        if (!message.isEmpty()) {
                            message += ", ";
                        }
                        message += stats2;
                    }
                    if (!message.isEmpty()) {
                        LOG.debug(message);
                    }
                }
            }
        });
    }

    private static class CountingPredicate<T> implements Predicate<T> {

        private final String name;
        private final Predicate<T> predicate;
        private int count, matches;

        public CountingPredicate(String name, Predicate<T> predicate) {
            this.name = name;
            this.predicate = predicate;
        }

        public String getStats() {
            return count == 0 ? "" : ("Predicate statistics for '" + name + "': " + matches + "/" + count);
        }

        @Override
        public boolean apply(T doc) {
            count += 1;
            boolean match = predicate.apply(doc);
            matches += (match ? 1 : 0);
            return match;
        }
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