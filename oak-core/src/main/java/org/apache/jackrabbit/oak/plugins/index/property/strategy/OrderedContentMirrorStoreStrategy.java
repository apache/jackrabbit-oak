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

package org.apache.jackrabbit.oak.plugins.index.property.strategy;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterators.singletonIterator;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ENTRY_COUNT_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;
import static org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.LANES;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.Predicate;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.ReadOnlyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Same as for {@link ContentMirrorStoreStrategy} but the order of the keys is kept by using the
 * following structure
 * 
 * <code>
 *  :index : {
 *      :start : { :next = n1 },
 *      n0 : { /content/foo/bar(match=true), :next=n3 },
 *      n1 : { /content/foo1/bar(match=true), :next=n0 },
 *      n2 : { /content/foo2/bar(match=true), :next= }, //this is the end of the list
 *      n3 : { /content/foo3/bar(match=true), :next=n2 }
 *  }
 * </code>
 */
public class OrderedContentMirrorStoreStrategy extends ContentMirrorStoreStrategy {
    /**
     * convenience property for initialising an empty multi-value :next
     */
    public static final Iterable<String> EMPTY_NEXT = 
            Collections.nCopies(OrderedIndex.LANES, "");
    
    /**
     * convenience property that represent an empty :next as array
     */
    public static final String[] EMPTY_NEXT_ARRAY = Iterables.toArray(EMPTY_NEXT, String.class);

    /**
     * the property linking to the next node
     */
    public static final String NEXT = ":next";

    /**
     * node that works as root of the index (start point or 0 element)
     */
    public static final String START = ":start";

    /**
     * a NodeState used for easy creating of an empty :start
     */
    public static final NodeState EMPTY_START_NODE = EmptyNodeState.EMPTY_NODE.builder()
        .setProperty(NEXT, EMPTY_NEXT, Type.STRINGS).getNodeState();

    private static final Logger LOG = LoggerFactory.getLogger(OrderedContentMirrorStoreStrategy.class);
    
    private static final Random RND = new Random(System.currentTimeMillis());
    
    /**
     * maximum number of attempt for potential recursive processes like seek() 
     */
    private static final int MAX_RETRIES = LANES+1;
        
    /**
     * the direction of the index.
     */
    private OrderDirection direction = OrderedIndex.DEFAULT_DIRECTION;
    

    public OrderedContentMirrorStoreStrategy() {
        super();
    }
    
    public OrderedContentMirrorStoreStrategy(OrderDirection direction) {
        this();
        this.direction = direction;
    }
    
    private static void printWalkedLanes(final String msg, final String[] walked) {
        if (LOG.isTraceEnabled()) {
            String m = (msg == null) ? "" : msg;
            if (walked == null) {
                LOG.trace(m + " walked: null");
            } else {
                for (int i = 0; i < walked.length; i++) {
                    LOG.trace("{}walked[{}]: {}", new Object[] { m, i, walked[i] });
                }
            }
        }
    }
    
    @Override
    NodeBuilder fetchKeyNode(@Nonnull NodeBuilder index, @Nonnull String key) {
        LOG.debug("fetchKeyNode() - === new item '{}'", key);
        // this is where the actual adding and maintenance of index's keys happen
        NodeBuilder node = null;
        NodeBuilder start = index.child(START);
        Predicate<String> condition = direction.isAscending() 
            ? new PredicateGreaterThan(key, true)
            : new PredicateLessThan(key, true);
        String[] walked = new String[OrderedIndex.LANES];
        
        if (Strings.isNullOrEmpty(getPropertyNext(start))) {
            // it means we're in an empty/new index. Setting properly the :start's :next
            setPropertyNext(start, EMPTY_NEXT_ARRAY);
        }
        
        // we use the seek for seeking the right spot. The walkedLanes will have all our
        // predecessors
        String entry = seek(index, condition, walked, 0, new FixingDanglingLinkCallback(index));
        if (LOG.isDebugEnabled()) {
            LOG.debug("fetchKeyNode() - entry: {} ", entry);
            printWalkedLanes("fetchKeyNode() - ", walked);
        }
        
        if (entry != null && entry.equals(key)) {
            // it's an existing node. We should not need to update anything around pointers
            LOG.debug("fetchKeyNode() - node already there.");
            node = index.getChildNode(key);
        } else {
            // the entry does not exits yet
            node = index.child(key);
            // it's a brand new node. let's start setting an empty next
            setPropertyNext(node, EMPTY_NEXT_ARRAY);
            int lane = getLane();
            LOG.debug("fetchKeyNode() - extracted lane: {}", lane);
            String next;
            NodeBuilder predecessor;
            for (int l = lane; l >= 0; l--) {
                // let's update the predecessors starting from the coin-flip lane
                predecessor = index.getChildNode(walked[l]);
                next = getPropertyNext(predecessor, l);
                setPropertyNext(predecessor, key, l);
                setPropertyNext(node, next, l);
                if (LOG.isDebugEnabled()) {
                    LOG.debug("fetchKeyNode() - on lane: {}", l);
                    LOG.debug("fetchKeyNode() - next from previous: {}", next);
                    LOG.debug("fetchKeyNode() - new status of predecessor name: {} - {} ", walked[l], predecessor.getProperty(NEXT));
                    LOG.debug("fetchKeyNode() - new node name: {} - {}", key, node.getProperty(NEXT));
                }
            }
        }
        return node;
    }
                                        
    @Override
    void prune(final NodeBuilder index, final Deque<NodeBuilder> builders, final String key) {
        LOG.debug("prune() - deleting: {}", key);
        for (NodeBuilder node : builders) {
            if (node.hasProperty("match") || node.getChildNodeCount(1) > 0) {
                return;
            } else if (node.exists()) {
                if (node.hasProperty(NEXT)) {
                    String[] walkedLanes = new String[OrderedIndex.LANES];
                    String entry;
                    String lane0Next, prevNext, currNext;
                    
                    // for as long as we have the an entry and we didn't update the lane0 we have
                    // to keep searching and update
                    do {
                        entry = seek(index,
                            new PredicateEquals(key),
                            walkedLanes,
                            0,
                            new LoggingDanglinLinkCallback()
                            );
                        lane0Next = getPropertyNext(index.getChildNode(walkedLanes[0]));
                        if (LOG.isDebugEnabled()) {
                            for (int i = 0; i < walkedLanes.length; i++) {
                                LOG.debug("prune() - walkedLanes[{}]: {}", i,
                                    walkedLanes[i]);
                            }
                        }
                        for (int lane = walkedLanes.length - 1; lane >= 0; lane--) {
                            prevNext = getPropertyNext(index.getChildNode(walkedLanes[lane]), lane);
                            if (key.equals(prevNext)) {
                                // if it's actually pointing to us let's deal with it
                                currNext = getPropertyNext(node, lane);
                                if (LOG.isDebugEnabled()) {
                                    LOG.debug(
                                        "prune() - setting next for '{}' on lane '{}' with '{}'",
                                        new Object[] {
                                        walkedLanes[lane],
                                        lane,
                                        currNext});
                                }
                                setPropertyNext(index.getChildNode(walkedLanes[lane]),
                                    currNext, lane);
                            }
                        }
                    } while (entry != null && !key.equals(lane0Next));
                }
                node.remove();
            }
        }
    }

    /**
     * retrieve an Iterable for going through the index in the right order without the :start node
     * 
     * @param index the root of the index (:index)
     * @return the iterable
     */
    @Override
    @Nonnull
    Iterable<? extends ChildNodeEntry> getChildNodeEntries(@Nonnull final NodeState index) {
        return getChildNodeEntries(index, false);
    }

    /**
     * Retrieve an Iterable for going through the index in the right order with potentially the
     * :start node
     * 
     * @param index the root of the index (:index)
     * @param includeStart true if :start should be included as first element
     * @return
     */
    @Nonnull
    Iterable<? extends ChildNodeEntry> getChildNodeEntries(@Nonnull final NodeState index,
                                                           final boolean includeStart) {
        Iterable<? extends ChildNodeEntry> cne = null;
        final NodeState start = index.getChildNode(START);

        String startNext = getPropertyNext(start); 
        if ((!start.exists() || Strings.isNullOrEmpty(startNext)) && !includeStart) {
            // if the property is not there or is empty it means we're empty
            cne = Collections.emptyList();
        } else {
            cne = new FullIterable(index, includeStart);
        }
        return cne;
    }

    /**
     * search the index for the provided PropertyRestriction
     * 
     * @param filter
     * @param indexName
     * @param indexMeta
     * @param pr
     * @return the iterable
     */
    public Iterable<String> query(final Filter filter, final String indexName,
                                  final NodeState indexMeta, final PropertyRestriction pr) {
        return query(filter, indexName, indexMeta, INDEX_CONTENT_NODE_NAME, pr);
    }

    /**
     * queries through the index as other query() but provides the PropertyRestriction to be applied
     * for advanced cases like range queries
     * 
     * @param filter
     * @param indexName
     * @param indexMeta
     * @param indexStorageNodeName
     * @param pr
     * @return the iterable
     */
    public Iterable<String> query(final Filter filter, final String indexName,
                                  final NodeState indexMeta, final String indexStorageNodeName,
                                  final PropertyRestriction pr) {
        if (LOG.isDebugEnabled()) {
            LOG.debug("query() - filter: {}", filter);            
            LOG.debug("query() - indexName: {}", indexName);            
            LOG.debug("query() - indexMeta: {}", indexMeta);            
            LOG.debug("query() - indexStorageNodeName: {}", indexStorageNodeName);            
            LOG.debug("query() - pr: {}", pr);            
        }
        
        final NodeState indexState = indexMeta.getChildNode(indexStorageNodeName);
        final NodeBuilder index = new ReadOnlyBuilder(indexState);
        final String firstEncoded = (pr.first == null) ? null 
                                                       : encode(pr.first.getValue(Type.STRING));
        final String lastEncoded = (pr.last == null) ? null
                                                     : encode(pr.last.getValue(Type.STRING));
        
        if (firstEncoded != null && !firstEncoded.equals(lastEncoded)) {
            // '>' & '>=' and between use case
            LOG.debug("'>' & '>=' and between use case");
            ChildNodeEntry firstValueableItem;
            String firstValuableItemKey;
            Iterable<String> it = Collections.emptyList();
            Iterable<ChildNodeEntry> childrenIterable;
            
            if (lastEncoded == null) {
                LOG.debug("> & >= case.");
                firstValuableItemKey = seek(index,
                    new PredicateGreaterThan(firstEncoded, pr.firstIncluding));
                if (firstValuableItemKey != null) {
                    firstValueableItem = new OrderedChildNodeEntry(firstValuableItemKey,
                        indexState.getChildNode(firstValuableItemKey));
                    if (direction.isAscending()) {
                        childrenIterable = new SeekedIterable(indexState, firstValueableItem);
                        it = new QueryResultsWrapper(filter, indexName, childrenIterable);
                    } else {
                        it = new QueryResultsWrapper(filter, indexName, new BetweenIterable(
                            indexState, firstValueableItem, firstEncoded,
                            pr.firstIncluding, direction));
                    }
                }
            } else {
                String first, last;
                boolean includeFirst, includeLast;
                first = firstEncoded;
                last = lastEncoded;
                includeFirst = pr.firstIncluding;
                includeLast = pr.lastIncluding;

                if (LOG.isDebugEnabled()) {
                    final String op1 = includeFirst ? ">=" : ">";
                    final String op2 = includeLast ? "<=" : "<";
                    LOG.debug("in between case. direction: {} - Condition: (x {} {} AND x {} {})",
                        new Object[] { direction, op1, first, op2, last });
                }

                if (direction.equals(OrderDirection.ASC)) {
                    firstValuableItemKey = seek(index,
                        new PredicateGreaterThan(first, includeFirst));
                } else {
                    firstValuableItemKey = seek(index,
                        new PredicateLessThan(last, includeLast));
                }
                
                LOG.debug("firstValueableItem: {}", firstValuableItemKey);
                
                if (firstValuableItemKey != null) {
                    firstValueableItem = new OrderedChildNodeEntry(firstValuableItemKey,
                        indexState.getChildNode(firstValuableItemKey));
                    childrenIterable = new BetweenIterable(indexState, firstValueableItem, last,
                        includeLast, direction);
                    it = new QueryResultsWrapper(filter, indexName, childrenIterable);
                }
            }

            return it;
        } else if (lastEncoded != null && !lastEncoded.equals(firstEncoded)) {
            // '<' & '<=' use case
            LOG.debug("'<' & '<=' use case");
            final String searchfor = lastEncoded;
            final boolean include = pr.lastIncluding;
            Predicate<String> predicate = new PredicateLessThan(searchfor, include);
            
            LOG.debug("< & <= case. - searchfor: {} - include: {} - predicate: {}",
                new Object[] { searchfor, include, predicate });

            ChildNodeEntry firstValueableItem;
            String firstValueableItemKey =  seek(index, predicate);
            
            LOG.debug("firstValuableItem: {}", firstValueableItemKey);
            
            Iterable<String> it = Collections.emptyList();
            if (firstValueableItemKey != null) {
                firstValueableItem = new OrderedChildNodeEntry(firstValueableItemKey,
                    indexState.getChildNode(firstValueableItemKey));
                if (direction.isAscending()) {
                    it = new QueryResultsWrapper(filter, indexName, new BetweenIterable(indexState,
                        firstValueableItem, searchfor, include, direction));
                } else {
                    it = new QueryResultsWrapper(filter, indexName, new SeekedIterable(indexState,
                        firstValueableItem));
                }
            }
            return it;
        } else if (firstEncoded != null && firstEncoded.equals(lastEncoded)) {
            // property = $value case
            LOG.debug("'property = $value' case");
            
            final NodeState key = indexState.getChildNode(firstEncoded);
            if (key.exists()) {
                return new Iterable<String>() {
                    @Override
                    public Iterator<String> iterator() {
                        PathIterator pi = new PathIterator(filter, indexName);
                        pi.setPathContainsValue(true);
                        pi.enqueue(singletonIterator(new MemoryChildNodeEntry(firstEncoded, key)));
                        return pi;
                    }
                };
            } else {
                return Collections.emptyList();
            }
        } else {
            // property is not null. AKA "open query"
            LOG.debug("property is not null. AKA 'open query'. FullIterable");
            return new QueryResultsWrapper(filter, indexName, new FullIterable(indexState, false));
        }
    }
    
    private static String encode(@Nonnull final String value) {
        checkNotNull(value);
        String v;
        try {
            v = URLEncoder.encode(value, Charsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            LOG.error("Error while encoding value.");
            v = value;
        }
        return v;
    }
        
    static class OrderedChildNodeEntry extends AbstractChildNodeEntry {
        private final String name;
        private final NodeState state;

        public OrderedChildNodeEntry(@Nonnull
        final String name, @Nonnull
        final NodeState state) {
            this.name = name;
            this.state = state;
        }

        @Override
        @Nonnull
        public String getName() {
            return name;
        }

        @Override
        @Nonnull
        public NodeState getNodeState() {
            return state;
        }
    }
    
    /**
     * estimate the number of nodes given the provided PropertyRestriction
     * 
     * @param indexMeta
     * @param pr
     * @param max
     * @return the estimated number of nodes
     */
    public long count(NodeState indexMeta, Filter.PropertyRestriction pr, int max) {
        long count = 0;
        NodeState content = indexMeta.getChildNode(INDEX_CONTENT_NODE_NAME);
        Filter.PropertyRestriction lpr = pr;
        
        if (content.exists()) {
            if (lpr == null) {
                // it means we have no restriction and we should return the whole lot
                lpr = new Filter.PropertyRestriction();
            }
            // the index is not empty
            String value;
            if (lpr.firstIncluding && lpr.lastIncluding && lpr.first != null
                && lpr.first.equals(lpr.last)) {
                // property==value case
                value = lpr.first.getValue(Type.STRING);
                NodeState n = content.getChildNode(value);
                if (n.exists()) {
                    CountingNodeVisitor v = new CountingNodeVisitor(max);
                    v.visit(n);
                    count = v.getEstimatedCount();
                }
            } else if (lpr.isNotNullRestriction()) {
                // property not null case
                PropertyState ec = indexMeta.getProperty(ENTRY_COUNT_PROPERTY_NAME);
                if (ec != null) {
                    count = ec.getValue(Type.LONG);
                } else {
                    CountingNodeVisitor v = new CountingNodeVisitor(max);
                    v.visit(content);
                    count = v.getEstimatedCount();
                }
            } else if (lpr.first != null && !lpr.first.equals(lpr.last)) {
                // > & >= in ascending index
                value = lpr.first.getValue(Type.STRING);
                final String vv = encode(value);
                final boolean include = lpr.firstIncluding;
                final OrderDirection dd = direction;

                Iterable<? extends ChildNodeEntry> children = getChildNodeEntries(content);
                Predicate<String> predicate = new Predicate<String>() {
                    private String v = vv;
                    private boolean i = include;
                    private OrderDirection d = dd;
                    
                    @Override
                    public boolean apply(String input) {
                        boolean b;
                        
                        if (d.equals(OrderDirection.ASC)) {
                            b = v.compareTo(input) > 0;
                        } else {
                            b = v.compareTo(input) < 0;
                        }
                        
                        b = b || (i && v.equals(input));
                        
                        return b;
                    }

                    @Override
                    public String getSearchFor() {
                        throw new UnsupportedOperationException();
                    }
                };

                CountingNodeVisitor v;
                int depthTotal = 0;
                // seeking the right starting point
                for (ChildNodeEntry child : children) {
                    String converted = child.getName();
                    if (predicate.apply(converted)) {
                        // here we are let's start counting
                        v = new CountingNodeVisitor(max);
                        v.visit(content.getChildNode(child.getName()));
                        count += v.getCount();
                        depthTotal += v.depthTotal;
                        if (count > max) {
                            break;
                        }
                    }
                }
                // small hack for having a common way of counting
                v = new CountingNodeVisitor(max);
                v.depthTotal = depthTotal;
                v.count = (int) Math.min(count, Integer.MAX_VALUE);
                count = v.getEstimatedCount();
            } else if (lpr.last != null && !lpr.last.equals(lpr.first)) {
                // < & <= 
                value = lpr.last.getValue(Type.STRING);
                final String vv = encode(value);
                final boolean include = lpr.lastIncluding;
                final OrderDirection dd = direction;
                
                Iterable<? extends ChildNodeEntry> children = getChildNodeEntries(content);
                Predicate<String> predicate = new Predicate<String>() {
                    private String v = vv;
                    private boolean i = include;
                    private OrderDirection d = dd;
                    
                    @Override
                    public boolean apply(String input) {
                        boolean b;
                        
                        if (d.equals(OrderDirection.ASC)) {
                            b = v.compareTo(input) < 0;
                        } else {
                            b = v.compareTo(input) > 0;
                        }
                        
                        b = b || (i && v.equals(input));
                        
                        return b;
                    }

                    @Override
                    public String getSearchFor() {
                        throw new UnsupportedOperationException();
                    }
                };
                
                CountingNodeVisitor v;
                int depthTotal = 0;
                // seeking the right starting point
                for (ChildNodeEntry child : children) {
                    String converted = child.getName();
                    if (predicate.apply(converted)) {
                        // here we are let's start counting
                        v = new CountingNodeVisitor(max);
                        v.visit(content.getChildNode(child.getName()));
                        count += v.getCount();
                        depthTotal += v.depthTotal;
                        if (count > max) {
                            break;
                        }
                    }
                }
                // small hack for having a common way of counting
                v = new CountingNodeVisitor(max);
                v.depthTotal = depthTotal;
                v.count = (int) Math.min(count, Integer.MAX_VALUE);
                count = v.getEstimatedCount();
            }

        }
        return count;
    }
        
    /**
     * wrap an {@code Iterable<ChildNodeEntry>} in something that can be understood by the Query
     * Engine
     */
    private static class QueryResultsWrapper implements Iterable<String> {
        private Iterable<ChildNodeEntry> children;
        private String indexName;
        private Filter filter;

        public QueryResultsWrapper(Filter filter, String indexName,
                                   Iterable<ChildNodeEntry> children) {
            this.children = children;
            this.indexName = indexName;
            this.filter = filter;
        }

        @Override
        public Iterator<String> iterator() {
            PathIterator pi = new PathIterator(filter, indexName);
            pi.setPathContainsValue(true);
            pi.enqueue(children.iterator());
            return pi;
        }
    }
    
    /**
     * iterating throughout the index in the correct order. Externalised as class for easy
     * overloading.
     */
    private static class FullIterator implements Iterator<ChildNodeEntry> {
        private boolean includeStart;
        private NodeState start;
        NodeState current;
        private NodeState index;
        private NodeBuilder builder;
        String currentName;
        private DanglingLinkCallback dlc = new LoggingDanglinLinkCallback();

        public FullIterator(NodeState index, NodeState start, boolean includeStart,
                            NodeState current) {
            this.includeStart = includeStart;
            this.start = start;
            this.current = current;
            this.index = index;
            this.builder = new ReadOnlyBuilder(index);
        }

        @Override
        public boolean hasNext() {
            String next = getPropertyNext(current);
            boolean hasNext = (includeStart && start.equals(current))
                || (!includeStart && !Strings.isNullOrEmpty(next)
                    && ensureAndCleanNode(
                                  builder, next, 
                                  currentName == null ? "" : currentName, 
                                  0,
                                  dlc));
                        
            return hasNext;
        }

        @Override
        public ChildNodeEntry next() {
            ChildNodeEntry entry = null;
            if (includeStart && start.equals(current)) {
                entry = new OrderedChildNodeEntry(START, current);
                // let's set it to false. We just included it.
                includeStart = false;
            } else {
                if (hasNext()) {
                    currentName = getPropertyNext(current);
                    current = index.getChildNode(currentName);
                    entry = new OrderedChildNodeEntry(currentName, current);
                } else {
                    throw new NoSuchElementException();
                }
            }
            
            return entry;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }

        /**
         * @return the name of the current node. May be null in some cases.
         */
        @Nullable
        String getCurrentName() {
            return currentName;
        }
    }
    
    /**
     * Convenience class for iterating throughout the index in the correct order
     */
    private static class FullIterable implements Iterable<ChildNodeEntry> {
        private boolean includeStart;

        NodeState index;
        NodeState start;
        NodeState current;

        /**
         * @param index the current index content state. The {@code :index} node
         * @param includeStart whether include {@code :start} or not.
         */
        public FullIterable(NodeState index, boolean includeStart) {
            this.index = index;
            this.includeStart = includeStart;
            NodeState s = index.getChildNode(START);
            if (includeStart && !s.exists()) {
                start = EMPTY_START_NODE;
            } else {
                start = s;
            }
            current = start;
        }

        @Override
        public Iterator<ChildNodeEntry> iterator() {
            return new FullIterator(index, start, includeStart, current);
        }
    }

    /**
     * Iterator that allows to start iterating from a given position
     */
    private static class SeekedIterator extends FullIterator {
        /**
         * whether the seekeed item has been returned already or not.
         */
        private boolean firstReturned;

        /**
         * the seeked item
         */
        private ChildNodeEntry first;

        public SeekedIterator(NodeState index, NodeState start, ChildNodeEntry first) {
            super(index, start, false, first.getNodeState());
            this.first = first;
        }

        @Override
        public boolean hasNext() {
            return !firstReturned || super.hasNext();
        }

        @Override
        public ChildNodeEntry next() {
            if (firstReturned) {
                return super.next();
            } else {
                currentName = first.getName();
                current = first.getNodeState();
                firstReturned = true;
                return first;
            }
        }
    }
    
    /**
     * iterable that starts at a provided position ({@code ChildNodeEntry})
     */
    private static class SeekedIterable extends FullIterable {
        ChildNodeEntry first;

        public SeekedIterable(NodeState index, ChildNodeEntry first) {
            super(index, false);
            this.first = first;
        }

        @Override
        public Iterator<ChildNodeEntry> iterator() {
            return new SeekedIterator(index, start, first);
        }
    }

    /**
     * see {@link #seek(NodeState, Predicate<ChildNodeEntry>, ChildNodeEntry[])} passing null as
     * last argument
     */
    String seek(@Nonnull NodeBuilder index, @Nonnull Predicate<String> condition) {
        return seek(index, condition, null, 0, new LoggingDanglinLinkCallback());
    }
    
    /**
     * seek for an element in the index given the provided Predicate. If {@code walkedLanes} won't
     * be null it will have on the way out the last elements of each lane walked through during the
     * seek.
     * 
     * @param index the index content node {@code :index}
     * @param condition the predicate to evaluate
     * @param walkedLanes if not null will contain the last element of the walked lanes with each
     *            lane represented by the corresponding position in the array. <b>You have</b> to
     *            pass in an array already sized as {@link OrderedIndex#LANES} or an
     *            {@link IllegalArgumentException} will be raised
     * @param retries the number of retries
     * @return the entry or null if not found
     */
    String seek(@Nonnull final NodeBuilder index,
                               @Nonnull final Predicate<String> condition,
                               @Nullable final String[] walkedLanes, 
                               int retries,
                               @Nullable DanglingLinkCallback callback) {
        boolean keepWalked = false;
        String searchfor = condition.getSearchFor();
        if (LOG.isDebugEnabled()) {
            LOG.debug("seek() - Searching for: {}", condition.getSearchFor());        
            LOG.debug("seek() - condition: {}", condition);
        }
        Predicate<String> walkingPredicate = direction.isAscending() 
                                                             ? new PredicateLessThan(searchfor, true)
                                                             : new PredicateGreaterThan(searchfor, true);
        // we always begin with :start
        String currentKey = START;
        String found = null;
        
        if (walkedLanes != null) {
            if (walkedLanes.length != OrderedIndex.LANES) {
                throw new IllegalArgumentException(String.format(
                    "Wrong size for keeping track of the Walked Lanes. Expected %d but was %d",
                    OrderedIndex.LANES, walkedLanes.length));
            }
            // ensuring the right data
            for (int i = 0; i < walkedLanes.length; i++) {
                walkedLanes[i] = currentKey;
            }
            keepWalked = true;
        }

        int lane;
        boolean stillLaning;
        String nextkey; 

        if ((direction.isAscending() && condition instanceof PredicateLessThan)
            || (direction.isDescending() && condition instanceof PredicateGreaterThan)) {
            // we're asking for a <, <= query from ascending index or >, >= from descending
            // we have to walk the lanes from bottom to up rather than up to bottom.
            
            LOG.debug("seek() - cross case");
            
            lane = 0;
            do {
                stillLaning = lane < OrderedIndex.LANES;
                nextkey = getPropertyNext(index.getChildNode(currentKey), lane);
                if ((Strings.isNullOrEmpty(nextkey) || !walkingPredicate.apply(nextkey)) && lane < OrderedIndex.LANES) {
                    // if we're currently pointing to NIL or the next element does not fit the search
                    // but we still have lanes left
                    lane++;
                } else {
                    if (condition.apply(nextkey)) {
                        // this branch is used so far only for range queries.
                        // while figuring out how to correctly reproduce the issue is less risky
                        // to leave this untouched.
                        found = nextkey;
                    } else {
                        currentKey = nextkey;
                        if (keepWalked && !Strings.isNullOrEmpty(currentKey) && walkedLanes != null) {
                            walkedLanes[lane] = currentKey;
                        }
                    }
                }
            } while (((!Strings.isNullOrEmpty(nextkey) && walkingPredicate.apply(nextkey)) || stillLaning) && (found == null));
        } else {
            LOG.debug("seek() - plain case");
            
            lane = OrderedIndex.LANES - 1;
            NodeBuilder currentNode = null;
            int iteration = 0;
            boolean exitCondition = true;
            
            do {
                iteration++;
                stillLaning = lane > 0;
                if (currentNode == null) {
                    currentNode = index.getChildNode(currentKey);
                }
                nextkey = getPropertyNext(currentNode, lane);
                if ((Strings.isNullOrEmpty(nextkey) || !walkingPredicate.apply(nextkey)) && lane > 0) {
                    // if we're currently pointing to NIL or the next element does not fit the search
                    // but we still have lanes left, let's lower the lane;
                    lane--;
                } else {
                    if (condition.apply(nextkey)) {
                        if (ensureAndCleanNode(index, nextkey, currentKey, lane, callback)) {
                            found = nextkey;
                        } else {
                            if (retries < MAX_RETRIES) {
                                return seek(index, condition, walkedLanes, ++retries, callback);
                            } else {
                                LOG.debug(
                                    "Attempted a lookup and fix for {} times. Leaving it be and returning null",
                                    retries);
                                return null;
                            }
                        }
                    } else {
                        currentKey = nextkey;
                        currentNode = null;
                        if (keepWalked && !Strings.isNullOrEmpty(currentKey) && walkedLanes != null) {
                            for (int l = lane; l >= 0; l--) {
                                walkedLanes[l] = currentKey;
                            }
                        }
                    }
                }
                
                exitCondition = ((!Strings.isNullOrEmpty(nextkey) && walkingPredicate
                    .apply(nextkey)) || stillLaning) && (found == null);
                
                if (LOG.isTraceEnabled()) {
                    LOG.trace("seek()::plain case - --- iteration: {}", iteration);
                    LOG.trace("seek()::plain case - retries: {},  MAX_RETRIES: {}", retries,
                        MAX_RETRIES);
                    LOG.trace("seek()::plain case - lane: {}", lane);
                    LOG.trace("seek()::plain case - currentKey: {}", currentKey);
                    LOG.trace("seek()::plain case - nextkey: {}", nextkey);
                    LOG.trace("seek()::plain case - condition.apply(nextkey): {}",
                        condition.apply(nextkey));
                    LOG.trace("seek()::plain case - found: {}", found);
                    LOG.trace("seek()::plain case - !Strings.isNullOrEmpty(nextkey): {}",
                        !Strings.isNullOrEmpty(nextkey));
                    LOG.trace("seek()::plain case - walkingPredicate.apply(nextkey): {}",
                        walkingPredicate.apply(nextkey));
                    LOG.trace("seek()::plain case - stillLaning: {}", stillLaning);
                    LOG.trace(
                        "seek()::plain case - While Condition: {}",
                        exitCondition);
                }
            } while (exitCondition);
        }
        
        return found;
    }
    
    /**
     * ensure that the provided {@code next} actually exists as node. Attempt to clean it up
     * otherwise.
     * 
     * @param index the {@code :index} node
     * @param next the {@code :next} retrieved for the provided lane
     * @param current the current node from which {@code :next} has been retrieved
     * @param lane the lane on which we're looking into
     * @return true if the node exists, false otherwise
     */
    private static boolean ensureAndCleanNode(@Nonnull final NodeBuilder index, 
                                              @Nonnull final String next,
                                              @Nonnull final String current,
                                              final int lane,
                                              @Nullable DanglingLinkCallback callback) {
        checkNotNull(index);
        checkNotNull(next);
        checkNotNull(current);
        checkArgument(lane < LANES && lane >= 0, "The lane must be between 0 and LANES");
        
        if (index.getChildNode(next).exists()) {
            return true;
        } else {
            if (callback != null) {
                callback.perform(current, next, lane);
            }
            return false;
        }
    }
    
    /**
     * predicate for evaluating 'key' equality across index 
     */
    static class PredicateEquals implements Predicate<String> {
        private String searchfor;

        public PredicateEquals(@Nonnull String searchfor) {
            this.searchfor = searchfor;
        }

        @Override
        public boolean apply(String arg0) {
            return !Strings.isNullOrEmpty(arg0) && searchfor.equals(arg0);
        }

        @Override
        public String getSearchFor() {
            return searchfor;
        }
    }
    
    /**
     * evaluates when the current element is greater than (>) and greater than equal
     * {@code searchfor}
     */
    static class PredicateGreaterThan implements Predicate<String> {
        private String searchforDecoded;
        private boolean include;
        
        public PredicateGreaterThan(@Nonnull String searchfor) {
            this(searchfor, false);
        }
        
        public PredicateGreaterThan(@Nonnull String searchfor, boolean include) {
            this.searchforDecoded = searchfor;
            this.include = include;
        }

        @Override
        public boolean apply(String arg0) {
            boolean b = false;
            if (!Strings.isNullOrEmpty(arg0)) {
                String name = arg0;
                b = searchforDecoded.compareTo(name) < 0 || 
                    (include && searchforDecoded.equals(name));
            }
            
            return b;
        }
        
        @Override
        public String getSearchFor() {
            return searchforDecoded;
        }
    }

    /**
     * evaluates when the current element is less than (<) and less than equal {@code searchfor}
     */
    static class PredicateLessThan implements Predicate<String> {
        private String searchforOriginal;
        private boolean include;

        public PredicateLessThan(@Nonnull String searchfor) {
            this(searchfor, false);
        }

        public PredicateLessThan(@Nonnull String searchfor, boolean include) {
            this.searchforOriginal = searchfor;
            this.include = include;
        }

        @Override
        public boolean apply(String arg0) {
            boolean b = false;
            if (!Strings.isNullOrEmpty(arg0)) {
                String name = arg0;
                b = searchforOriginal.compareTo(name) > 0
                    || (include && searchforOriginal.equals(name));
            }
            
            return b;
        }
        
        @Override
        public String getSearchFor() {
            return searchforOriginal;
        }
    }
    
    /**
     * iterable for going through a set of data in the case of BETWEEN queries. We don't have to
     * return more data for having the Query Engine to skip them later.
     */
    private static class BetweenIterable extends SeekedIterable {
        private String lastKey;
        private boolean lastInclude;
        private OrderDirection direction;
        
        public BetweenIterable(final NodeState index, final ChildNodeEntry first,
                               final String lastKey, final boolean lastInclude,
                               final OrderDirection direction) {
            super(index, first);
            this.lastKey = lastKey;
            this.lastInclude = lastInclude;
            this.direction = direction;
        }

        @Override
        public Iterator<ChildNodeEntry> iterator() {
            return new BetweenIterator(index, start, first, lastKey, lastInclude, direction);
        }
    }

    /**
     * iterator for iterating in the cases of BETWEEN queries.
     */
    private static class BetweenIterator extends SeekedIterator {
        private Predicate<String> condition;
        
        /**
         * @param index the current index content {@code :index}
         * @param start the {@code :start} node
         * @param first the first valuable options for starting iterating from.
         * @param lastKey the last key to be returned
         * @param lastInclude whether including the last key or not. 
         */
        public BetweenIterator(final NodeState index, final NodeState start,
                               final ChildNodeEntry first, final String lastKey,
                               final boolean lastInclude, final OrderDirection direction) {
            super(index, start, first);
            this.condition = new Predicate<String>() {
                private String v = lastKey;
                private boolean i = lastInclude;
                private OrderDirection d = direction;
                
                @Override
                public boolean apply(String input) {
                    // splitting in multiple lines for easier debugging
                    boolean compareTo, equals, apply;
                    if (d.equals(OrderDirection.ASC)) {
                        compareTo = v.compareTo(input) > 0;
                    } else {
                        compareTo = v.compareTo(input) < 0;
                    }
                    
                    equals =  v.equals(input);
                    apply = compareTo || (i && equals);
                    return apply;
                }

                @Override
                public String getSearchFor() {
                    throw new UnsupportedOperationException();
                }
            };
        }

        @Override
        public boolean hasNext() {
            boolean hasNext = super.hasNext();
            String name = getCurrentName();
            
            if (name != null && hasNext) {
                String next = getPropertyNext(current);
                hasNext = hasNext && condition.apply(next);
            }
            return hasNext;
        }
    }

    /**
     * set the value of the the :next at position 0
     * 
     * @param node the node to modify
     * @param next the 'next' value
     */
    static void setPropertyNext(@Nonnull final NodeBuilder node, final String... next) {
        if (node != null && next != null) {
            int len = next.length - 1;
            for (; len >= 0; len--) {
                if (next[len].length() != 0) {
                    break;
                }
            }
            len++;
            List<String> list = new ArrayList<String>(len);
            for (int i = 0; i < len; i++) {
                list.add(next[i]);
            }
            node.setProperty(NEXT, list, Type.STRINGS);
        }
    }
    
    /**
     * set the value of the :next at the given position. If the property :next won't be there by the
     * time this method is invoked it won't perform any action
     * 
     * @param node
     * @param value
     * @param lane
     */
    public static void setPropertyNext(@Nonnull final NodeBuilder node, 
                                final String value, final int lane) {
        if (node != null && value != null && lane >= 0 && lane < OrderedIndex.LANES) {
            PropertyState next = node.getProperty(NEXT);
            if (next != null) {
                String[] values;
                if (next.isArray()) {
                    values = Iterables.toArray(next.getValue(Type.STRINGS), String.class);
                    if (values.length < OrderedIndex.LANES) {
                        // it could be we increased the number of lanes and running on some existing
                        // content
                        LOG.debug("topping-up the number of lanes.");
                        List<String> vv = Lists.newArrayList(values);
                        for (int i = vv.size(); i < OrderedIndex.LANES; i++) {
                            vv.add("");
                        }
                        values = vv.toArray(new String[0]);
                    }
                } else {
                    values = Iterables.toArray(EMPTY_NEXT, String.class);
                    values[0] = next.getValue(Type.STRING);
                }
                values[lane] = value;
                setPropertyNext(node, values);
            }
        }
    }

    /**
     * see {@link #getPropertyNext(NodeState, int)} by providing '0' as lane
     */
    static String getPropertyNext(@Nonnull final NodeState nodeState) {
        return getPropertyNext(nodeState, 0);
    }
    
    /**
     * return the 'next' value at the provided position
     * 
     * @param nodeState the node state to inspect
     * @return the next value
     */
    static String getPropertyNext(@Nonnull final NodeState state, final int lane) {
        return getPropertyNext(new ReadOnlyBuilder(state), lane);
    }
    
    /**
     * short-cut for using NodeBuilder. See {@code getNext(NodeState)}
     */
    public static String getPropertyNext(@Nonnull final NodeBuilder node) {
        return getPropertyNext(node, 0);
    }

    /**
     * short-cut for using NodeBuilder. See {@code getNext(NodeState)}
     */
    public static String getPropertyNext(@Nonnull final NodeBuilder node, final int lane) {
        checkNotNull(node);
        
        String next = "";
        PropertyState ps = node.getProperty(NEXT);
        if (ps != null) {
            if (ps.isArray()) {
                int count = ps.count();
                if (count > 0 && count > lane) {
                    next = ps.getValue(Type.STRING, lane);
                }
            } else {
                next = ps.getValue(Type.STRING);
            }
        }
        return next;
    }

    /**
     * retrieve the lane to be updated based on probabilistic approach.
     * 
     * Having 4 lanes if we have the 3 to be updated it means we'll have to update lanes
     * 0,1,2 and 3. If we'll have 2 only 0,1,2 and so on.
     * 
     * Lane 0 will always be updated as it's the base linked list.
     * 
     * It uses {@code getLane(Random)} by passing a {@code new Random(System.currentTimeMillis())}
     * 
     * @see OrderedIndex.DEFAULT_PROBABILITY
     * 
     * @return the lane to start updating from.
     */
    public int getLane() {
        return getLane(RND);
    }
    
    /**
     * used for mocking purposes or advanced uses. Use the {@code getLane()} where possible
     * 
     * @param rnd the Random generator to be used for probability
     * @return the lane to be updated. 
     */
    protected int getLane(@Nonnull final Random rnd) {
        final int maxLanes = OrderedIndex.LANES - 1;
        int lane = 0;
        
        while (rnd.nextDouble() < OrderedIndex.DEFAULT_PROBABILITY && lane < maxLanes) {
            lane++;
        }
        
        return lane;
    }
    
    /**
     * implementors of this interface will deal with the dangling link cases along the list
     * (OAK-2077)
     */
    interface DanglingLinkCallback {
        /**
         * perform the required operation on the provided {@code current} node for the {@code next}
         * value on {@code lane}
         * 
         * @param current the current node with the dangling link
         * @param next the value pointing to the missing node
         * @param lane the lane on which the link is on
         */
        void perform(String current, String next, int lane);
    }
    
    /**
     * implements a "Read-only" version for managing the dangling links which will simply track down
     * in logs the presence of it
     */
    static class LoggingDanglinLinkCallback implements DanglingLinkCallback {
        private boolean alreadyLogged;
        
        @Override
        public void perform(@Nonnull final String current, 
                            @Nonnull final String next, 
                            int lane) {
            checkNotNull(next);
            checkNotNull(current);
            checkArgument(lane < LANES && lane >= 0, "The lane must be between 0 and LANES");

            if (!alreadyLogged) {
                LOG.warn(
                    "Dangling link to '{}' found on lane '{}' for key '{}'. Trying to clean it up. You may consider a reindex",
                    new Object[] { next, lane, current });
                alreadyLogged = true;
            }
        }
    }
    
    static class FixingDanglingLinkCallback extends LoggingDanglinLinkCallback {
        private final NodeBuilder indexContent;
        
        public FixingDanglingLinkCallback(@Nonnull final NodeBuilder indexContent) {
            this.indexContent = checkNotNull(indexContent);
        }

        @Override
        public void perform(String current, String next, int lane) {
            super.perform(current, next, lane);
            // as we're already pointing to nowhere it's safe to truncate here and avoid
            // future errors. We'll fix all the lanes from slowest to fastest starting from the lane
            // with the error. This should keep the list a bit more consistent with what is
            // expected.
            for (int l = lane; l < LANES; l++) {
                setPropertyNext(indexContent.getChildNode(current), "", lane);                
            }
        }
    }
}