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
package org.apache.jackrabbit.oak.plugins.mongomk;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.AbstractMap.SimpleImmutableEntry;

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.TreeMap;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Maps;
import com.google.common.collect.PeekingIterator;
import org.apache.jackrabbit.oak.plugins.mongomk.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The revision history for a given property. The history may span multiple
 * previous documents.
 */
class PropertyHistory implements Iterable<NodeDocument> {

    private static final Logger LOG = LoggerFactory.getLogger(PropertyHistory.class);

    private final DocumentStore store;
    private final NodeDocument main;
    private final String property;
    private final Revision revision;
    private final String id;

    public PropertyHistory(@Nonnull DocumentStore store,
                           @Nonnull NodeDocument main,
                           @Nonnull String property,
                           @Nullable Revision revision) {
        this.store = checkNotNull(store);
        this.main = checkNotNull(main);
        this.property = checkNotNull(property);
        this.revision = revision;
        this.id = main.getId();
    }

    @Override
    public Iterator<NodeDocument> iterator() {
        Iterable<Map.Entry<Revision, Range>> ranges;
        if (revision != null) {
            ranges = Iterables.filter(
                    main.getPreviousRanges().entrySet(),
                    new Predicate<Map.Entry<Revision, Range>>() {
                        @Override
                        public boolean apply(Map.Entry<Revision, Range> input) {
                            return input.getValue().includes(revision);
                        }
                    });
        } else {
            ranges = main.getPreviousRanges().entrySet();
        }

        Iterable<Map.Entry<Revision, NodeDocument>> docs = Iterables.transform(ranges,
                new Function<Map.Entry<Revision, Range>, Map.Entry<Revision, NodeDocument>>() {
            @Nullable
            @Override
            public Map.Entry<Revision, NodeDocument> apply(Map.Entry<Revision, Range> input) {
                Revision r = input.getKey();
                String prevId = Utils.getPreviousIdFor(id, r);
                NodeDocument prev = store.find(Collection.NODES, prevId);
                if (prev == null) {
                    LOG.warn("Document with previous revisions not found: " + prevId);
                    return null;
                }
                return new SimpleImmutableEntry<Revision, NodeDocument>(r, prev);
            }
        });

        // filter out null docs and ensure order
        return ensureOrder(Iterables.filter(docs, Predicates.notNull()));
    }

    /**
     * Ensures the order of docs is correct with respect to the highest revision
     * for each ValueMap for the given property.
     *
     * @param docs the docs to order.
     * @return the docs in the correct order.
     */
    private Iterator<NodeDocument> ensureOrder(final Iterable<Map.Entry<Revision, NodeDocument>> docs) {
        return new Iterator<NodeDocument>() {
            PeekingIterator<Map.Entry<Revision, NodeDocument>> input
                    = Iterators.peekingIterator(docs.iterator());
            TreeMap<Revision, NodeDocument> queue = Maps.newTreeMap(StableRevisionComparator.INSTANCE);
            NodeDocument next = fetchNext();

            @Override
            public boolean hasNext() {
                return next != null;
            }

            @Override
            public NodeDocument next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                NodeDocument doc = next;
                next = fetchNext();
                return doc;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }

            private NodeDocument fetchNext() {
                refillQueue();
                if (queue.isEmpty()) {
                    return null;
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
                        if (!values.isEmpty() && (revision == null || values.containsKey(revision))) {
                            // put into queue with first (highest) revision
                            // from value map
                            queue.put(values.keySet().iterator().next(), doc);
                        }
                    }
                }
            }
        };
    }
}
