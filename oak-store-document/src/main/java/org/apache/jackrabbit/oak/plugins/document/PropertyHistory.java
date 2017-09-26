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
package org.apache.jackrabbit.oak.plugins.document;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static java.util.AbstractMap.SimpleImmutableEntry;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.PeekingIterator;
import org.apache.jackrabbit.oak.plugins.document.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The revision history for a given property. The history may span multiple
 * previous documents.
 */
class PropertyHistory implements Iterable<NodeDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(PropertyHistory.class);

    private final NodeDocument doc;
    private final String property;
    // path of the main document
    private final String mainPath;

    public PropertyHistory(@Nonnull NodeDocument doc,
                           @Nonnull String property) {
        this.doc = checkNotNull(doc);
        this.property = checkNotNull(property);
        this.mainPath = doc.getMainPath();
    }

    @Override
    public Iterator<NodeDocument> iterator() {
        return ensureOrder(filter(transform(doc.getPreviousRanges().entrySet(),
                new Function<Map.Entry<Revision, Range>, Map.Entry<Revision, NodeDocument>>() {
            @Nullable
            @Override
            public Map.Entry<Revision, NodeDocument> apply(Map.Entry<Revision, Range> input) {
                Revision r = input.getKey();
                int h = input.getValue().height;
                String prevId = Utils.getPreviousIdFor(mainPath, r, h);
                NodeDocument prev = doc.getPreviousDocument(prevId);
                if (prev == null) {
                    LOG.debug("Document with previous revisions not found: " + prevId);
                    return null;
                }
                return new SimpleImmutableEntry<Revision, NodeDocument>(r, prev);
            }
        }), Predicates.notNull()));
    }

    /**
     * Ensures the order of docs is correct with respect to the highest revision
     * for each ValueMap for the given property.
     *
     * @param docs the docs to order.
     * @return the docs in the correct order.
     */
    private Iterator<NodeDocument> ensureOrder(final Iterable<Map.Entry<Revision, NodeDocument>> docs) {
        return new AbstractIterator<NodeDocument>() {
            PeekingIterator<Map.Entry<Revision, NodeDocument>> input
                    = Iterators.peekingIterator(docs.iterator());
            TreeMap<Revision, NodeDocument> queue =
                    new TreeMap<Revision, NodeDocument>(StableRevisionComparator.INSTANCE);

            @Override
            protected NodeDocument computeNext() {
                refillQueue();
                if (queue.isEmpty()) {
                    return endOfData();
                } else {
                    return queue.remove(queue.lastKey());
                }
            }

            /**
             * Refill the queue until the highest entry in the queue is higher
             * than the peeked entry from the input iterator.
             */
            private void refillQueue() {
                for (;;) {
                    // the doc to enqueue
                    NodeDocument doc;
                    if (queue.isEmpty()) {
                        if (input.hasNext()) {
                            doc = input.next().getValue();
                        } else {
                            // no more input -> done
                            return;
                        }
                    } else {
                        // peek first and compare with queue
                        if (input.hasNext()) {
                            if (queue.comparator().compare(queue.lastKey(), input.peek().getKey()) < 0) {
                                doc = input.next().getValue();
                            } else {
                                // top of queue rev is higher than input -> done
                                return;
                            }
                        } else {
                            // no more input -> done
                            return;
                        }
                    }
                    // check if the revision is actually in there
                    if (doc != null) {
                        Map<Revision, String> values = doc.getValueMap(property);
                        Iterator<Revision> revs = values.keySet().iterator();
                        if (revs.hasNext()) {
                            // put into queue with first (highest) revision
                            // from value map
                            queue.put(revs.next(), doc);
                        }
                    }
                }
            }
        };
    }
}
