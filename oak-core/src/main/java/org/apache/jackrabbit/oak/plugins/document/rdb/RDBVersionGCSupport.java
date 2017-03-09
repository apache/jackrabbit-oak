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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.jackrabbit.oak.plugins.document.Collection;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument.SplitDocType;
import org.apache.jackrabbit.oak.plugins.document.VersionGCSupport;
import org.apache.jackrabbit.oak.plugins.document.rdb.RDBDocumentStore.QueryCondition;

import com.google.common.base.Predicate;
import com.google.common.collect.AbstractIterator;

/**
 * RDB specific version of {@link VersionGCSupport} which uses an extended query
 * interface to fetch required {@link NodeDocument}s.
 */
public class RDBVersionGCSupport extends VersionGCSupport {
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
        return getIterator(RDBDocumentStore.EMPTY_KEY_PATTERN, conditions);
    }

    private Iterable<NodeDocument> getSplitDocuments() {
        List<QueryCondition> conditions = Collections.emptyList();
        // absent support for SDTYPE as indexed property: exclude those
        // documents from the query which definitively aren't split documents
        List<String> excludeKeyPatterns = Arrays.asList("_:/%", "__:/%", "___:/%");
        return getIterator(excludeKeyPatterns, conditions);
    }

    @Override
    protected Iterable<NodeDocument> identifyGarbage(final Set<SplitDocType> gcTypes, final long oldestRevTimeStamp) {
        return filter(getSplitDocuments(), new Predicate<NodeDocument>() {
            @Override
            public boolean apply(NodeDocument doc) {
                return gcTypes.contains(doc.getSplitDocType()) && doc.hasAllRevisionLessThan(oldestRevTimeStamp);
            }
        });
    }

    private Iterable<NodeDocument> getIterator(final List<String> excludeKeyPatterns, final List<QueryCondition> conditions) {
        return new Iterable<NodeDocument>() {
            @Override
            public Iterator<NodeDocument> iterator() {
                return new AbstractIterator<NodeDocument>() {

                    private static final int BATCH_SIZE = 100;
                    private String startId = NodeDocument.MIN_ID_VALUE;
                    private Iterator<NodeDocument> batch = nextBatch();

                    @Override
                    protected NodeDocument computeNext() {
                        // read next batch if necessary
                        if (!batch.hasNext()) {
                            batch = nextBatch();
                        }

                        NodeDocument doc;
                        if (batch.hasNext()) {
                            doc = batch.next();
                            // remember current id
                            startId = doc.getId();
                        } else {
                            doc = endOfData();
                        }
                        return doc;
                    }

                    private Iterator<NodeDocument> nextBatch() {
                        List<NodeDocument> result = store.query(Collection.NODES, startId, NodeDocument.MAX_ID_VALUE,
                                excludeKeyPatterns, conditions, BATCH_SIZE);
                        return result.iterator();
                    }
                };
            }
        };
    }
}
