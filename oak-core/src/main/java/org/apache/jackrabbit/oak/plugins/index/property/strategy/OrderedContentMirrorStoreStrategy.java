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

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.ENTRY_COUNT_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_CONTENT_NODE_NAME;

import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
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
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

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
    public static final Iterable<String> EMPTY_NEXT = ImmutableList.of("", "", "", "");
    
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
    
    @Override
    NodeBuilder fetchKeyNode(@Nonnull NodeBuilder index, @Nonnull String key) {
        // this is where the actual adding and maintenance of index's keys happen
        NodeBuilder node = null;
        NodeBuilder start = index.child(START);
        Predicate<ChildNodeEntry> condition = direction.isAscending() 
            ? new PredicateGreaterThan(key, true)
            : new PredicateLessThan(key, true);
        ChildNodeEntry[] walked = new ChildNodeEntry[OrderedIndex.LANES];
        
        if (Strings.isNullOrEmpty(getPropertyNext(start))) {
            // it means we're in an empty/new index. Setting properly the :start's :next
            setPropertyNext(start, EMPTY_NEXT_ARRAY);
        }
        
        // we use the seek for seeking the right spot. The walkedLanes will have all our
        // predecessors
        ChildNodeEntry entry = seek(index.getNodeState(), condition, walked);
        if (entry != null && entry.getName().equals(key)) {
            // it's an existing node. We should not need to update anything around pointers
            node = index.getChildNode(key);
        } else {
            // the entry does not exits yet
            node = index.child(key);
            // it's a brand new node. let's start setting an empty next
            setPropertyNext(node, EMPTY_NEXT_ARRAY);
            int lane = getLane();
            String next;
            NodeBuilder predecessor;
            for (int l = lane; l >= 0; l--) {
                // let's update the predecessors starting from the coin-flip lane
                predecessor = index.getChildNode(walked[l].getName());
                next = getPropertyNext(predecessor, l);
                setPropertyNext(predecessor, key, l);
                setPropertyNext(node, next, l);
            }
        }
        return node;
    }
                                        
    @Override
    void prune(final NodeBuilder index, final Deque<NodeBuilder> builders, final String key) {
        for (NodeBuilder node : builders) {
            if (node.hasProperty("match") || node.getChildNodeCount(1) > 0) {
                return;
            } else if (node.exists()) {
                if (node.hasProperty(NEXT)) {
                    ChildNodeEntry[] walkedLanes = new ChildNodeEntry[OrderedIndex.LANES];
                    ChildNodeEntry entry;
                    String lane0Next, prevNext, currNext;
                    
                    // for as long as we have the an entry and we didn't update the lane0 we have
                    // to keep searching and update
                    do {
                        entry = seek(index.getNodeState(),
                            new PredicateEquals(key),
                            walkedLanes
                            );
                        lane0Next = getPropertyNext(walkedLanes[0]);
                        for (int lane = walkedLanes.length - 1; lane >= 0; lane--) {
                            prevNext = getPropertyNext(walkedLanes[lane], lane);
                            if (key.equals(prevNext)) {
                                // if it's actually pointing to us let's deal with it
                                currNext = getPropertyNext(node, lane);
                                setPropertyNext(index.getChildNode(walkedLanes[lane].getName()),
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
        
        final NodeState index = indexMeta.getChildNode(indexStorageNodeName);

        if (pr.first != null && !pr.first.equals(pr.last)) {
            // '>' & '>=' and between use case
            ChildNodeEntry firstValueableItem;
            Iterable<String> it = Collections.emptyList();
            Iterable<ChildNodeEntry> childrenIterable;
            
            if (pr.last == null) {
                LOG.debug("> & >= case.");
                firstValueableItem = seek(index,
                    new PredicateGreaterThan(pr.first.getValue(Type.STRING), pr.firstIncluding));
                if (firstValueableItem != null) {
                    if (direction.isAscending()) {
                        childrenIterable = new SeekedIterable(index, firstValueableItem);
                        it = new QueryResultsWrapper(filter, indexName, childrenIterable);
                    } else {
                        it = new QueryResultsWrapper(filter, indexName, new BetweenIterable(index,
                            firstValueableItem, pr.first.getValue(Type.STRING), pr.firstIncluding,
                            direction));
                    }
                }
            } else {
                String first, last;
                boolean includeFirst, includeLast;
                first = pr.first.getValue(Type.STRING);
                last = pr.last.getValue(Type.STRING);
                includeFirst = pr.firstIncluding;
                includeLast = pr.lastIncluding;

                if (LOG.isDebugEnabled()) {
                    final String op1 = includeFirst ? ">=" : ">";
                    final String op2 = includeLast ? "<=" : "<";
                    LOG.debug("in between case. direction: {} - Condition: (x {} {} AND x {} {})",
                        new Object[] { direction, op1, first, op2, last });
                }

                if (direction.equals(OrderDirection.ASC)) {
                    firstValueableItem = seek(index,
                        new PredicateGreaterThan(first, includeFirst));
                } else {
                    firstValueableItem = seek(index,
                        new PredicateLessThan(last, includeLast));
                }
                
                LOG.debug("firstValueableItem: {}", firstValueableItem);
                
                if (firstValueableItem != null) {
                    childrenIterable = new BetweenIterable(index, firstValueableItem, last,
                        includeLast, direction);
                    it = new QueryResultsWrapper(filter, indexName, childrenIterable);
                }
            }

            return it;
        } else if (pr.last != null && !pr.last.equals(pr.first)) {
            // '<' & '<=' use case
            final String searchfor = pr.last.getValue(Type.STRING);
            final boolean include = pr.lastIncluding;
            Predicate<ChildNodeEntry> predicate = new PredicateLessThan(searchfor, include);
            
            LOG.debug("< & <= case. - searchfor: {} - include: {} - predicate: {}",
                new Object[] { searchfor, include, predicate });

            ChildNodeEntry firstValueableItem = seek(index, predicate);
            
            LOG.debug("firstValuableItem: {}", firstValueableItem);
            
            Iterable<String> it = Collections.emptyList();
            if (firstValueableItem != null) {
                if (direction.isAscending()) {
                    it = new QueryResultsWrapper(filter, indexName, new BetweenIterable(index,
                        firstValueableItem, searchfor, include, direction));
                } else {
                    it = new QueryResultsWrapper(filter, indexName, new SeekedIterable(index,
                        firstValueableItem));
                }
            }
            return it;
        } else {
            // property is not null. AKA "open query"
            Iterable<String> values = null;
            return query(filter, indexName, indexMeta, values);
        }
    }

    private static String convert(String value) {
        return value.replaceAll("%3A", ":");
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
            } else if (lpr.first == null && lpr.last == null) {
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
                final String vv = value;
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
                    String converted = convert(child.getName());
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
                final String vv = value;
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
                    String converted = convert(child.getName());
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
        String currentName;

        public FullIterator(NodeState index, NodeState start, boolean includeStart,
                            NodeState current) {
            this.includeStart = includeStart;
            this.start = start;
            this.current = current;
            this.index = index;
        }

        @Override
        public boolean hasNext() {
            return (includeStart && start.equals(current))
                   || (!includeStart && !Strings.isNullOrEmpty(getPropertyNext(current)));
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
    ChildNodeEntry seek(@Nonnull NodeState index,
                                      @Nonnull Predicate<ChildNodeEntry> condition) {
        return seek(index, condition, null);
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
     * @return the entry or null if not found
     */
    ChildNodeEntry seek(@Nonnull final NodeState index,
                               @Nonnull final Predicate<ChildNodeEntry> condition,
                               @Nullable final ChildNodeEntry[] walkedLanes) {
        boolean keepWalked = false;
        String searchfor = condition.getSearchFor();
        LOG.debug("seek() - Searching for: {}", condition.getSearchFor());        
        Predicate<ChildNodeEntry> walkingPredicate = direction.isAscending() 
                                                             ? new PredicateLessThan(searchfor, true)
                                                             : new PredicateGreaterThan(searchfor, true);
        // we always begin with :start
        ChildNodeEntry current = new OrderedChildNodeEntry(START, index.getChildNode(START));
        ChildNodeEntry found = null;
        
        if (walkedLanes != null) {
            if (walkedLanes.length != OrderedIndex.LANES) {
                throw new IllegalArgumentException(String.format(
                    "Wrong size for keeping track of the Walked Lanes. Expected %d but was %d",
                    OrderedIndex.LANES, walkedLanes.length));
            }
            // ensuring the right data
            for (int i = 0; i < walkedLanes.length; i++) {
                walkedLanes[i] = current;
            }
            keepWalked = true;
        }

        int lane;
        boolean stillLaning;
        String nextkey; 
        ChildNodeEntry next;

        if ((direction.isAscending() && condition instanceof PredicateLessThan)
            || (direction.isDescending() && condition instanceof PredicateGreaterThan)) {
            // we're asking for a <, <= query from ascending index or >, >= from descending
            // we have to walk the lanes from bottom to up rather than up to bottom.
            
            lane = 0;
            do {
                stillLaning = lane < OrderedIndex.LANES;
                nextkey = getPropertyNext(current, lane);
                next = (Strings.isNullOrEmpty(nextkey)) 
                    ? null 
                    : new OrderedChildNodeEntry(nextkey, index.getChildNode(nextkey));
                if ((next == null || !walkingPredicate.apply(next)) && lane < OrderedIndex.LANES) {
                    // if we're currently pointing to NIL or the next element does not fit the search
                    // but we still have lanes left
                    lane++;
                } else {
                    if (condition.apply(next)) {
                        found = next;
                    } else {
                        current = next;
                        if (keepWalked && current != null) {
                            walkedLanes[lane] = current;
                        }
                    }
                }
            } while (((next != null && walkingPredicate.apply(next)) || stillLaning) && (found == null));
        } else {
            lane = OrderedIndex.LANES - 1;
            
            do {
                stillLaning = lane > 0;
                nextkey = getPropertyNext(current, lane);
                next = (Strings.isNullOrEmpty(nextkey)) 
                    ? null 
                    : new OrderedChildNodeEntry(nextkey, index.getChildNode(nextkey));
                if ((next == null || !walkingPredicate.apply(next)) && lane > 0) {
                    // if we're currently pointing to NIL or the next element does not fit the search
                    // but we still have lanes left, let's lower the lane;
                    lane--;
                } else {
                    if (condition.apply(next)) {
                        found = next;
                    } else {
                        current = next;
                        if (keepWalked && current != null) {
                            for (int l = lane; l >= 0; l--) {
                                walkedLanes[l] = current;
                            }
                        }
                    }
                }
            } while (((next != null && walkingPredicate.apply(next)) || stillLaning) && (found == null));
        }
        
        return found;
    }
    
    /**
     * predicate for evaluating 'key' equality across index 
     */
    static class PredicateEquals implements Predicate<ChildNodeEntry> {
        private String searchfor;

        public PredicateEquals(@Nonnull String searchfor) {
            this.searchfor = searchfor;
        }

        @Override
        public boolean apply(ChildNodeEntry arg0) {
            return arg0 != null && searchfor.equals(arg0.getName());
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
    static class PredicateGreaterThan implements Predicate<ChildNodeEntry> {
        private String searchfor;
        private boolean include;
        
        public PredicateGreaterThan(@Nonnull String searchfor) {
            this(searchfor, false);
        }
        
        public PredicateGreaterThan(@Nonnull String searchfor, boolean include) {
            this.searchfor = searchfor;
            this.include = include;
        }

        @Override
        public boolean apply(ChildNodeEntry arg0) {
            boolean b = false;
            if (arg0 != null) {
                String name = convert(arg0.getName());
                b = searchfor.compareTo(name) < 0 || (include && searchfor
                        .equals(name));
            }
            
            return b;
        }
        
        @Override
        public String getSearchFor() {
            return searchfor;
        }
    }

    /**
     * evaluates when the current element is less than (<) and less than equal {@code searchfor}
     */
    static class PredicateLessThan implements Predicate<ChildNodeEntry> {
        private String searchfor;
        private boolean include;

        public PredicateLessThan(@Nonnull String searchfor) {
            this(searchfor, false);
        }

        public PredicateLessThan(@Nonnull String searchfor, boolean include) {
            this.searchfor = searchfor;
            this.include = include;
        }

        @Override
        public boolean apply(ChildNodeEntry arg0) {
            boolean b = false;
            if (arg0 != null) {
                String name = convert(arg0.getName());
                b = searchfor.compareTo(name) > 0 || (include && searchfor.equals(name));
            }

            return b;
        }
        
        @Override
        public String getSearchFor() {
            return searchfor;
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
            String n1 = (next.length > 0) ? next[0] : "";
            String n2 = (next.length > 1) ? next[1] : "";
            String n3 = (next.length > 2) ? next[2] : "";
            String n4 = (next.length > 3) ? next[3] : "";
            
            node.setProperty(NEXT, ImmutableList.of(n1, n2, n3, n4), Type.STRINGS);
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
    static void setPropertyNext(@Nonnull final NodeBuilder node, 
                                final String value, final int lane) {
        if (node != null && value != null && lane >= 0 && lane < OrderedIndex.LANES) {
            PropertyState next = node.getProperty(NEXT);
            if (next != null) {
                String[] values = Iterables.toArray(next.getValue(Type.STRINGS), String.class);
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
        String next = "";
        PropertyState ps = state.getProperty(NEXT);
        if (ps != null) {
            next = (lane < OrderedIndex.LANES) ? ps.getValue(Type.STRING, lane)
                                               : "";
        }
        return next;
    }
    
    /**
     * short-cut for using NodeBuilder. See {@code getNext(NodeState)}
     */
    static String getPropertyNext(@Nonnull final NodeBuilder node) {
        return getPropertyNext(node.getNodeState());
    }

    /**
     * short-cut for using NodeBuilder. See {@code getNext(NodeState)}
     */
    static String getPropertyNext(@Nonnull final NodeBuilder node, final int lane) {
        return getPropertyNext(node.getNodeState(), lane);
    }

    /**
     * short-cut for using ChildNodeEntry. See {@code getNext(NodeState)}
     */
    static String getPropertyNext(@Nonnull final ChildNodeEntry child) {
        return getPropertyNext(child.getNodeState());
    }

    static String getPropertyNext(@Nonnull final ChildNodeEntry child, int lane) {
        return getPropertyNext(child.getNodeState(), lane);
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
    int getLane(@Nonnull final Random rnd) {
        final int maxLanes = OrderedIndex.LANES - 1;
        int lane = 0;
        
        while (rnd.nextDouble() < OrderedIndex.DEFAULT_PROBABILITY && lane < maxLanes) {
            lane++;
        }
        
        return lane;
    }
}