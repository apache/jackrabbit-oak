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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.Filter.PropertyRestriction;
import org.apache.jackrabbit.oak.spi.state.AbstractChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;

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
                                                                              .setProperty(NEXT, "")
                                                                              .getNodeState();

    private static final Logger LOG = LoggerFactory.getLogger(OrderedContentMirrorStoreStrategy.class);

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
        NodeBuilder localkey = null;
        NodeBuilder start = index.child(START);

        // identifying the right place for insert
        String n = start.getString(NEXT);
        if (Strings.isNullOrEmpty(n)) {
            // new/empty index
            localkey = index.child(key);
            localkey.setProperty(NEXT, "");
            start.setProperty(NEXT, key);
        } else {
            // specific use-case where the item has to be added as first of the list
            String nextKey = n;
            Iterable<? extends ChildNodeEntry> children = getChildNodeEntries(index.getNodeState(),
                                                                              true);
            for (ChildNodeEntry child : children) {
                nextKey = child.getNodeState().getString(NEXT);
                if (Strings.isNullOrEmpty(nextKey)) {
                    // we're at the last element, therefore our 'key' has to be appended
                    index.getChildNode(child.getName()).setProperty(NEXT, key);
                    localkey = index.child(key);
                    localkey.setProperty(NEXT, "");
                } else {
                    if (isInsertHere(key, nextKey)) {
                        index.getChildNode(child.getName()).setProperty(NEXT, key);
                        localkey = index.child(key);
                        localkey.setProperty(NEXT, nextKey);
                        break;
                    }
                }
            }
        }

        return localkey;
    }

    /**
     * tells whether or not the is right to insert here a new item.
     * 
     * @param newItemKey the new item key to be added
     * @param existingItemKey the 'here' of the existing index
     * @return true for green light on insert false otherwise.
     */
    private boolean isInsertHere(@Nonnull String newItemKey, @Nonnull String existingItemKey) {
        if (OrderDirection.ASC.equals(direction)) {
            return newItemKey.compareTo(existingItemKey) < 0;
        } else {
            return newItemKey.compareTo(existingItemKey) > 0;
        }
    }
                                        
    @Override
    void prune(final NodeBuilder index, final Deque<NodeBuilder> builders) {
        for (NodeBuilder node : builders) {
            if (node.hasProperty("match") || node.getChildNodeCount(1) > 0) {
                return;
            } else if (node.exists()) {
                if (node.hasProperty(NEXT)) {
                    // it's an index key and we have to relink the list
                    // (1) find the previous element
                    ChildNodeEntry previous = findPrevious(
                            index.getNodeState(), node.getNodeState());
                    LOG.debug("previous: {}", previous);
                    // (2) find the next element
                    String next = node.getString(NEXT); 
                    if (next == null) {
                        next = "";
                    }
                    // (3) re-link the previous to the next
                    index.getChildNode(previous.getName()).setProperty(NEXT, next); 
                } 
                node.remove();
            }
        }
    }

    /**
     * find the previous item (ChildNodeEntry) in the index given the provided NodeState for
     * comparison
     * 
     * in an index sorted in ascending manner where we have @{code [1, 2, 3, 4, 5]} if we ask for 
     * a previous given 4 it will be 3. previous(4)=3.
     * 
     * in an index sorted in descending manner where we have @{code [5, 4, 3, 2, 1]} if we as for
     * a previous given 4 it will be 5. previous(4)=5.
     * 
     * @param index the index we want to look into ({@code :index})
     * @param node the node we want to compare
     * @return the previous item or null if not found.
     */
    @Nullable
    ChildNodeEntry findPrevious(@Nonnull final NodeState index, @Nonnull final NodeState node) {
        ChildNodeEntry previous = null;
        ChildNodeEntry current = null;
        boolean found = false;
        Iterator<? extends ChildNodeEntry> it = getChildNodeEntries(index, true).iterator();

        while (!found && it.hasNext()) {
            current = it.next();
            if (previous == null) {
                // first iteration
                previous = current;
            } else {
                found = node.equals(current.getNodeState());
                if (!found) {
                    previous = current;
                }
            }
        }

        return found ? previous : null;
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

        if ((!start.exists() || Strings.isNullOrEmpty(start.getString(NEXT))) && !includeStart) {
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
            // '>' & '>=' use case
            ChildNodeEntry firstValueableItem = seek(index,
                new PredicateGreaterThan(pr.first.getValue(Type.STRING), pr.firstIncluding));
            Iterable<String> it = Collections.emptyList();
            if (firstValueableItem != null) {
                Iterable<ChildNodeEntry> childrenIterable = (pr.last == null) ? new SeekedIterable(
                    index, firstValueableItem) : new BetweenIterable(index, firstValueableItem,
                    pr.last.getValue(Type.STRING), pr.lastIncluding);
                it = new QueryResultsWrapper(filter, indexName, childrenIterable);
            }
            return it;
        } else if (pr.last != null && !pr.last.equals(pr.first)) {
            // '<' & '<=' use case
            ChildNodeEntry firstValueableItem = seek(index,
                new PredicateLessThan(pr.last.getValue(Type.STRING), pr.lastIncluding));
            Iterable<String> it = Collections.emptyList();
            if (firstValueableItem != null) {
                it = new QueryResultsWrapper(filter, indexName, new SeekedIterable(index,
                    firstValueableItem));
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
    
    private static final class OrderedChildNodeEntry extends AbstractChildNodeEntry {
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
            } else if (lpr.first != null && !lpr.first.equals(lpr.last)
                       && OrderDirection.ASC.equals(direction)) {
                // > & >= in ascending index
                Iterable<? extends ChildNodeEntry> children = getChildNodeEntries(content);
                CountingNodeVisitor v;
                value = lpr.first.getValue(Type.STRING);
                int depthTotal = 0;
                // seeking the right starting point
                for (ChildNodeEntry child : children) {
                    String converted = convert(child.getName());
                    if (value.compareTo(converted) < 0
                        || (lpr.firstIncluding && value.equals(converted))) {
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
            } else if (lpr.last != null && !lpr.last.equals(lpr.first)
                       && OrderDirection.DESC.equals(direction)) {
                // > & >= in ascending index
                Iterable<? extends ChildNodeEntry> children = getChildNodeEntries(content);
                CountingNodeVisitor v;
                value = lpr.last.getValue(Type.STRING);
                int depthTotal = 0;
                // seeking the right starting point
                for (ChildNodeEntry child : children) {
                    String converted = convert(child.getName());
                    if (value.compareTo(converted) > 0
                        || (lpr.lastIncluding && value.equals(converted))) {
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
        private NodeState current;
        private NodeState index;
        private String currentName;

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
                   || (!includeStart && !Strings.isNullOrEmpty(current.getString(NEXT)));
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
                    currentName = current.getString(NEXT);
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
     * seek for an element in the index given the provided Predicate
     * 
     * @param index the index content node {@code :index}
     * @param condition the predicate to evaluate
     * @return the entry or null if not found
     */
    static ChildNodeEntry seek(@Nonnull NodeState index,
                                      @Nonnull Predicate<ChildNodeEntry> condition) {
        
        // TODO the FullIterable will have to be replaced with something else once we'll have the
        // Skip part of the list implemented.
        Iterable<ChildNodeEntry> children = new FullIterable(index, false);
        ChildNodeEntry entry = null;
        for (ChildNodeEntry child : children) {
            if (condition.apply(child)) {
                entry = child;
                break;
            }
        }
        return entry;
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
    }
    
    /**
     * iterable for going through a set of data in the case of BETWEEN queries. We don't have to
     * return more data for having the Query Engine to skip them later.
     */
    private static class BetweenIterable extends SeekedIterable {
        private String lastKey;
        private boolean lastInclude;
        
        public BetweenIterable(NodeState index, ChildNodeEntry first, String lastKey,
                               boolean lastInclude) {
            super(index, first);
            this.lastKey = lastKey;
            this.lastInclude = lastInclude;
        }

        @Override
        public Iterator<ChildNodeEntry> iterator() {
            return new BetweenIterator(index, start, first, lastKey, lastInclude);
        }
    }

    /**
     * iterator for iterating in the cases of BETWEEN queries.
     */
    private static class BetweenIterator extends SeekedIterator {
        private String lastKey;
        private boolean lastInclude;

        /**
         * @param index the current index content {@code :index}
         * @param start the {@code :start} node
         * @param first the first valuable options for starting iterating from.
         * @param lastKey the last key to be returned
         * @param lastInclude whether including the last key or not. 
         */
        public BetweenIterator(NodeState index, NodeState start, ChildNodeEntry first,
                               String lastKey, boolean lastInclude) {
            super(index, start, first);
            this.lastInclude = lastInclude;
            this.lastKey = lastKey;
        }

        @Override
        public boolean hasNext() {
            boolean next = super.hasNext();
            String name = getCurrentName();

            if (name != null && next) {
                name = convert(name);
                next = next && (lastKey.compareTo(name) > 0 || (lastInclude && lastKey
                    .equals(name)));
            }
            return next;
        }
    }
}