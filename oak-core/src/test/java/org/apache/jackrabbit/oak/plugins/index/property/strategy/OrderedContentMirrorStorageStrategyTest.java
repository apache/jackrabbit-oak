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

import static com.google.common.collect.Sets.newHashSet;
import static org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy.NEXT;
import static org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy.START;
import static org.easymock.EasyMock.createNiceMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Set;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy.OrderedChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.index.property.strategy.OrderedContentMirrorStoreStrategy.PredicateLessThan;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.query.ast.Operator;
import org.apache.jackrabbit.oak.query.index.FilterImpl;
import org.apache.jackrabbit.oak.spi.query.Filter;
import org.apache.jackrabbit.oak.spi.query.PropertyValues;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

/**
 *
 */
public class OrderedContentMirrorStorageStrategyTest {
    private static final Logger LOG = LoggerFactory.getLogger(OrderedContentMirrorStorageStrategyTest.class);
    
    /**
     * ascending ordered set of keys. Useful for testing
     */
    private static final String[] KEYS = new String[] { "000", "001", "002", "003", "004", "005",
                                                       "006", "007", "008", "009", "010", "011",
                                                       "012", "013", "014", };
    private static final Set<String> EMPTY_KEY_SET = newHashSet();
    private static final NumberFormat NF = new DecimalFormat("00000");
    
    /**
     * convenience class for mocking some behaviours while testing
     */
    private static class MockOrderedContentMirrorStoreStrategy extends
        OrderedContentMirrorStoreStrategy {
        
        public MockOrderedContentMirrorStoreStrategy() {
            super();
        }

        public MockOrderedContentMirrorStoreStrategy(OrderDirection direction) {
            super(direction);
        }

        /**
         * tells the code to use the Original class method implementation
         */
        public static final int SUPER_LANE = -1;
        private int lane = SUPER_LANE;

        @Override
        public int getLane() {
            if (lane == SUPER_LANE) {
                return super.getLane();
            } else {
                return lane;
            }
        }
        
        public void setLane(int lane) {
            this.lane = lane; 
        }
    }

    /**
     * checks that the fist item/key is inserted with an empty property 'next'
     *
     * expected structure:
     *
     * <code>
     * :index : {
     *    :start : { :next=n0 },
     *    n0 : {
     *       :next=,
     *       foo : {
     *          bar: { match=true}
     *       }
     *    }
     * }
     * </code>
     */
    @Test
    public void firstAndOnlyItem() {
        final String path = "/foo/bar";
        final String[] pathNodes = Iterables.toArray(PathUtils.elements(path), String.class);
        final String no = KEYS[0];

        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder node = null;

        store.update(index, path, EMPTY_KEY_SET, newHashSet(no));

        assertFalse(":index should be left alone with not changes", index.hasProperty(NEXT));
        node = index.getChildNode(START);
        assertTrue(":index should have the :start node", node.exists());
        assertEquals(":start should point to n0", no,
            Iterables.toArray(node.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);

        node = index.getChildNode(no);
        assertTrue("n0 should exists in the index", node.exists());
        assertEquals("n0 should point nowhere as it's the last (and only) element", "",
            Iterables.toArray(node.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);

        // checking content structure below n0
        node = node.getChildNode(pathNodes[0]);
        assertTrue("n0 should contain 'foo'", node.exists());
        node = node.getChildNode(pathNodes[1]);
        assertTrue("'foo' should contain 'bar'", node.exists());
        assertTrue("the 'foo' node should have 'match=true'", node.getBoolean("match"));
    }

    /**
     * test the saving of 2 new keys that comes already ordered
     *
     * final state of the index will be
     *
     * <code>
     *    :index : {
     *       :start : { :next=n0 },
     *       n0 : { :next=n1 },
     *       n1 : { :next= }
     *    }
     * </code>
     */
    @Test
    public void first2newKeysAlreadyOrdered() {
        final String path = "/foo/bar";
        final String n0 = KEYS[0];
        final String n1 = KEYS[1];

        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder node = null;

        // first node arrives
        store.update(index, path, EMPTY_KEY_SET, newHashSet(n0));

        node = index.getChildNode(START);
        assertTrue(":index should have :start", node.exists());
        assertEquals(":start should point to n0", n0, 
            Iterables.toArray(node.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);

        node = index.getChildNode(n0);
        assertTrue(":index should have n0", node.exists());
        assertEquals("n0 should point nowhere at this stage", "",
            Iterables.toArray(node.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);

        // second node arrives
        store.update(index, path, EMPTY_KEY_SET, newHashSet(n1));

        node = index.getChildNode(START);
        assertTrue(":index should still have :start", node.exists());
        assertEquals(":start should still point to n0", n0,
            Iterables.toArray(node.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);

        node = index.getChildNode(n0);
        assertTrue("n0 should still exists", node.exists());
        assertEquals("n0 should point to n1", n1,
            Iterables.toArray(node.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);

        node = index.getChildNode(n1);
        assertTrue("n1 should exists", node.exists());
        assertEquals("n1 should point nowhere", "",
            Iterables.toArray(node.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);
    }

    /**
     * Test the iteration of an empty index
     */
    @Test
    public void childNodeEntriesEmptyIndex() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeState index = EmptyNodeState.EMPTY_NODE;

        @SuppressWarnings("unchecked")
        Iterable<ChildNodeEntry> children = (Iterable<ChildNodeEntry>) store.getChildNodeEntries(index);

        assertNotNull("A returned Iterable cannot be null", children);
    }

    /**
     * test the iteration of the index with 2 shuffled items
     *
     * <code>
     *    :index : {
     *       :start : { :next=n1 },
     *       n0 : { :next= },
     *       n1 : { :next=n0 }
     *    }
     * </code>
     */
    @SuppressWarnings("unchecked")
    @Test
    public void childNodeEntriesACoupleOfMixedItems() {
        final String n0 = KEYS[1];
        final String n1 = KEYS[0];
        final NodeState node0 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, "").getNodeState();
        final NodeState node1 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, n0).getNodeState();
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();

        // setting-up the index structure
        index.child(START).setProperty(NEXT, n1);
        index.setChildNode(n0, node0);
        index.setChildNode(n1, node1);

        NodeState indexState = index.getNodeState();

        Iterable<ChildNodeEntry> children = (Iterable<ChildNodeEntry>) store.getChildNodeEntries(indexState);
        assertNotNull("The iterable cannot be null", children);
        assertEquals("Expecting 2 items in the index", 2, Iterators.size(children.iterator()));

        // ensuring the right sequence
        ChildNodeEntry entry = null;
        children = (Iterable<ChildNodeEntry>) store.getChildNodeEntries(indexState);
        Iterator<ChildNodeEntry> it = children.iterator();
        assertTrue("We should have 2 elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The first element should be n1", n1, entry.getName());
        assertEquals("Wrong entry returned", node1, entry.getNodeState());
        assertTrue("We should have 1 elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The second element should be n0", n0, entry.getName());
        assertEquals("Wrong entry returned", node0, entry.getNodeState());
        assertFalse("We should have be at the end of the list", it.hasNext());
    }

    /**
     * test the iteration of the index with 2 shuffled items without the :start
     * node
     *
     * <code>
     *    :index : {
     *       :start : { :next=n1 },
     *       n0 : { :next= },
     *       n1 : { :next=n0 }
     *    }
     * </code>
     */
    @SuppressWarnings("unchecked")
    @Test
    public void childNodeEntriesACoupleOfMixedItemsNoStart() {
        final String n0 = KEYS[1];
        final String n1 = KEYS[0];
        final NodeState node0 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, "").getNodeState();
        final NodeState node1 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, n0).getNodeState();
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();

        // setting-up the index structure
        index.child(START).setProperty(NEXT, n1);
        index.setChildNode(n0, node0);
        index.setChildNode(n1, node1);

        NodeState indexState = index.getNodeState();

        Iterable<ChildNodeEntry> children = (Iterable<ChildNodeEntry>) store.getChildNodeEntries(indexState, false);
        assertNotNull("The iterable cannot be null", children);
        assertEquals("Expecting 2 items in the index", 2, Iterators.size(children.iterator()));

        // ensuring the right sequence
        ChildNodeEntry entry = null;
        children = (Iterable<ChildNodeEntry>) store.getChildNodeEntries(indexState);
        Iterator<ChildNodeEntry> it = children.iterator();
        assertTrue("We should have 2 elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The first element should be n1", n1, entry.getName());
        assertEquals("Wrong entry returned", node1, entry.getNodeState());
        assertTrue("We should have 1 elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The second element should be n0", n0, entry.getName());
        assertEquals("Wrong entry returned", node0, entry.getNodeState());
        assertFalse("We should have be at the end of the list", it.hasNext());
    }

    /**
     * test the iteration of the index with 2 shuffled items including the
     * :start node as first
     *
     * <code>
     *    :index : {
     *       :start : { :next=n1 },
     *       n0 : { :next= },
     *       n1 : { :next=n0 }
     *    }
     * </code>
     */
    @SuppressWarnings("unchecked")
    @Test
    public void childNodeEntriesACoupleOfMixedItemsWithStart() {
        final String n0 = KEYS[1];
        final String n1 = KEYS[0];
        final NodeState nodeStart = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, n1).getNodeState();
        final NodeState node0 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, "").getNodeState();
        final NodeState node1 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, n0).getNodeState();
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();

        // setting-up the index structure
        index.setChildNode(START, nodeStart);
        index.setChildNode(n0, node0);
        index.setChildNode(n1, node1);

        NodeState indexState = index.getNodeState();

        Iterable<ChildNodeEntry> children = (Iterable<ChildNodeEntry>) store.getChildNodeEntries(indexState, true);
        assertNotNull("The iterable cannot be null", children);
        assertEquals("Expecting 3 items in the index", 3, Iterators.size(children.iterator()));

        // ensuring the right sequence
        ChildNodeEntry entry = null;
        children = (Iterable<ChildNodeEntry>) store.getChildNodeEntries(indexState, true);
        Iterator<ChildNodeEntry> it = children.iterator();
        assertTrue("We should still have elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The first element should be :start", START, entry.getName());
        assertEquals("Wrong entry returned", nodeStart, entry.getNodeState());
        assertTrue("We should still have elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The second element should be n1", n1, entry.getName());
        assertEquals("Wrong entry returned", node1, entry.getNodeState());
        assertTrue("We should still have elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The third element should be n0", n0, entry.getName());
        assertEquals("Wrong entry returned", node0, entry.getNodeState());
        assertFalse("We should be at the end of the list", it.hasNext());
    }

    /**
     * test the iteration over an empty list when the :start is required. In
     * this case :start should always be returned
     *
     * <code>
     *    :index : {
     *       :start : { :next= }
     *    }
     * </code>
     */
    @Test
    public void childNodeEntriesNoItemsWithStart() {
        NodeState nodeStart = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, "").getNodeState();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();

        // setting-up the index
        index.setChildNode(START, nodeStart);

        Iterable<? extends ChildNodeEntry> children = store.getChildNodeEntries(index.getNodeState(), true);
        assertEquals("Wrong size of Iterable", 1, Iterators.size(children.iterator()));

        Iterator<? extends ChildNodeEntry> it = store.getChildNodeEntries(index.getNodeState(), true).iterator();
        assertTrue("We should have at least 1 element", it.hasNext());
        ChildNodeEntry entry = it.next();
        assertEquals(":start is expected", START, entry.getName());
        assertEquals("wrong node returned", nodeStart, entry.getNodeState());
        assertFalse("We should be at the end of the list", it.hasNext());
    }

    /**
     * test the case where we want an iterator for the children of a brand new
     * index. In this case :start doesn't exists but if we ask for it we should
     * return it.
     */
    @Test
    public void childNodeEntriesNewIndexWithStart() {
        NodeState nodeStart = EmptyNodeState.EMPTY_NODE.builder()
            .setProperty(NEXT, OrderedContentMirrorStoreStrategy.EMPTY_NEXT, Type.STRINGS)
            .getNodeState();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();

        Iterator<? extends ChildNodeEntry> children = store.getChildNodeEntries(index.getNodeState(), true).iterator();
        assertEquals("Wrong number of children", 1, Iterators.size(children));

        children = store.getChildNodeEntries(index.getNodeState(), true).iterator();
        assertTrue("at least one item expected", children.hasNext());
        ChildNodeEntry child = children.next();
        assertEquals(START, child.getName());
        assertEquals(nodeStart, child.getNodeState());
        assertFalse(children.hasNext());
    }

    /**
     * test the insert of two shuffled items
     *
     * Building final a structure like
     *
     * <code>
     *    :index : {
     *       :start : { :next=n1 },
     *       n0 : { :next= },
     *       n1 : { :next=n0 }
     *    }
     * </code>
     *
     * where:
     *
     * <code>
     *    Stage 1
     *    =======
     *
     *    :index : {
     *       :start : { :next = n0 },
     *       n0 : {
     *          :next =
     *       }
     *    }
     *
     *    Stage 2
     *    =======
     *
     *    :index : {
     *       :start : { :next = n1 },
     *       n0 : {
     *          :next =
     *       },
     *       n1 : {
     *          :next = n0
     *       }
     *    }
     * </code>
     */
    @Test
    public void twoShuffledItems() {
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeState root = EmptyNodeState.EMPTY_NODE;
        NodeBuilder index = root.builder();
        String key1st = KEYS[1];
        String key2nd = KEYS[0];
        NodeState ns = null;

        // Stage 1
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(key1st));
        ns = index.getChildNode(START).getNodeState();
        assertEquals(":start is expected to point to the 1st node", key1st,
            Iterables.toArray(ns.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);
        ns = index.getChildNode(key1st).getNodeState();
        assertTrue("At Stage 1 the first node is expected to point nowhere as it's the last",
                        Strings.isNullOrEmpty(ns.getString(NEXT)));

        // Stage 2
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(key2nd));
        ns = index.getChildNode(START).getNodeState();
        assertEquals(":start is expected to point to the 2nd node", key2nd,
            Iterables.toArray(ns.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);
        ns = index.getChildNode(key1st).getNodeState();
        assertTrue("At stage 2 the first element should point nowhere as it's the last",
                        Strings.isNullOrEmpty(ns.getString(NEXT)));
        ns = index.getChildNode(key2nd).getNodeState();
        assertEquals("At Stage 2 the second element should point to the first one", key1st,
            Iterables.toArray(ns.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);
    }

    /**
     * test the insert of shuffled items
     *
     * Building a final structure like
     *
     * <code>
     *    {
     *       :start : { :next = n1 },
     *       n0 : {
     *          :next = ""
     *       },
     *       n1 : {
     *          :next = n2
     *       },
     *       n2 : {
     *          :next = n0
     *       }
     *    }
     * </code>
     *
     * where:
     *
     * <code>
     *    Stage 1
     *    =======
     *
     *    {
     *       :start : { :next = n0 },
     *       n0 : {
     *          :next =
     *       }
     *    }
     *
     *    Stage 2
     *    =======
     *
     *    {
     *       :start : { :next = n1 },
     *       n0 : { :next = },
     *       n1 : { :next = n0 }
     *    }
     *
     *    Stage 3
     *    =======
     *
     *    {
     *       :start : { :next = n1 },
     *       n0 : { :next = },
     *       n1 : { :next = n2 },
     *       n2 : { :next = n0 }
     *    }
     *
     *    Stage 4
     *    =======
     *
     *    {
     *       :start : { :next = n1 },
     *       n0 : { :next = n3 },
     *       n1 : { :next = n2 },
     *       n2 : { :next = n0 },
     *       n3 : { :next = }
     *    }
     * </code>
     */
    @Test
    public void fourShuffledElements() {
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[2];
        String n1 = KEYS[0];
        String n2 = KEYS[1];
        String n3 = KEYS[3];

        // Stage 1
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        NodeState n = index.getChildNode(START).getNodeState();
        assertEquals(":start should point to the first node", n0, 
            Iterables.toArray(n.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);
        assertTrue("the first node should point nowhere", Strings.isNullOrEmpty(index.getChildNode(n0).getString(NEXT)));

        // Stage 2
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        n = index.getChildNode(START).getNodeState();
        assertEquals(":start should point to n1", n1, 
            Iterables.toArray(n.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);
        n = index.getChildNode(n1).getNodeState();
        assertEquals("'n1' should point to 'n0'", n0,
            Iterables.toArray(n.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);
        assertTrue("n0 should still be point nowhere", Strings.isNullOrEmpty(index.getChildNode(n0).getString(NEXT)));

        // Stage 3
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        n = index.getChildNode(START).getNodeState();
        assertEquals(":start should point to n1", n1, 
            Iterables.toArray(n.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);
        n = index.getChildNode(n1).getNodeState();
        assertEquals("n1 should be pointing to n2", n2, 
            Iterables.toArray(n.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);
        n = index.getChildNode(n2).getNodeState();
        assertEquals("n2 should be pointing to n0", n0,
            Iterables.toArray(n.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);
        n = index.getChildNode(n0).getNodeState();
        assertTrue("n0 should still be the last item of the list", Strings.isNullOrEmpty(Iterables
            .toArray(n.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]));

        // Stage 4
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));
        n = index.getChildNode(START).getNodeState();
        assertEquals(":start should point to n1", n1, 
            Iterables.toArray(n.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);
        n = index.getChildNode(n1).getNodeState();
        assertEquals("n1 should be pointing to n2", n2, 
            Iterables.toArray(n.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0]);
        assertEquals("n2 should be pointing to n0", n0, getNext(index.getChildNode(n2)));
        assertEquals("n0 should be pointing to n3", n3, getNext(index.getChildNode(n0)));
        assertTrue("n3 should be the last element",
            Strings.isNullOrEmpty(getNext(index.getChildNode(n3))));
    }

    /**
     * perform a test where the index gets updated if an already existent
     * node/key gets updated by changing the key and the key contains only 1
     * item.
     *
     * Where the second key is greater than the first.
     *
     * <code>
     *    Stage 1
     *    =======
     *
     *    :index : {
     *       :start { :next = n0 },
     *       n0 : {
     *          :next =,
     *          content : {
     *             foobar : {
     *                match = true
     *             }
     *          }
     *       }
     *    }
     *
     *    Stage 2
     *    =======
     *
     *    :index : {
     *       :start : { :next = n1 },
     *       n1 : {
     *          :next =,
     *          content : {
     *             foobar : {
     *                match = true
     *             }
     *          }
     *       }
     *    }
     * </code>
     */

    @Test
    public void singleKeyUpdate() {
        final String n0 = KEYS[0];
        final String n1 = KEYS[1];
        final String path = "/content/foobar";
        final String[] nodes = Iterables.toArray(PathUtils.elements(path), String.class);
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder node = null;

        // Stage 1
        store.update(index, path, EMPTY_KEY_SET, newHashSet(n0));
        node = index.getChildNode(START);
        assertTrue(":start should exists", node.exists());
        assertEquals(":start should point to n0", n0, getNext(node));

        node = index.getChildNode(n0);
        assertTrue(":index should have n0", node.exists());
        assertTrue("n0 should point nowhere", Strings.isNullOrEmpty(getNext(node)));

        node = node.getChildNode(nodes[0]);
        assertTrue("n0 should have /content", node.exists());

        node = node.getChildNode(nodes[1]);
        assertTrue("/content should contain /foobar", node.exists());
        assertTrue("/foobar should have match=true", node.getBoolean("match"));

        // Stage 2
        store.update(index, path, newHashSet(n0), newHashSet(n1));
        node = index.getChildNode(START);
        assertEquals(":start should now point to n1", n1, getNext(node));

        node = index.getChildNode(n1);
        assertTrue("n1 should exists", node.exists());
        assertTrue("n1 should point nowhere", Strings.isNullOrEmpty(getNext(node)));

        node = node.getChildNode(nodes[0]);
        assertTrue("n1 should have /content", node.exists());

        node = node.getChildNode(nodes[1]);
        assertTrue("/content should contain /foobar", node.exists());
        assertTrue("/foobar should have match=true", node.getBoolean("match"));
    }

    /**
     * test the use case where a document change the indexed property. For
     * example document that change author.
     *
     * <p>
     * <i>it relies on the functionality of the store.update() for creating the
     * index</i>
     * </p>
     *
     * <code>
     *    Stage 1
     *    =======
     *
     *    :index : {
     *       :start : { :next = n0 },
     *       n0 : {
     *          :next = ,
     *          content : {
     *             one { match=true },
     *             two { match=true }
     *          }
     *       }
     *    }
     *
     *    Stage 2
     *    =======
     *
     *    :index : {
     *       :start : { :next = n0 },
     *       n0 : {
     *          :next = n1,
     *          content : {
     *             one : { match = true }
     *          }
     *       },
     *       n1 : {
     *          :next = ,
     *          content : {
     *             two : { match = true }
     *          }
     *       }
     *    }
     * </code>
     */
    @Test
    public void documentChangingKey() {
        final String path0 = "/content/one";
        final String path1 = "/content/two";
        final String n0 = KEYS[0];
        final String n1 = KEYS[1];
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();

        // Stage 1 - initialising the index
        store.update(index, path0, EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, path1, EMPTY_KEY_SET, newHashSet(n0));

        // ensuring the right structure
        assertTrue(index.hasChildNode(START));
        assertTrue(index.hasChildNode(n0));
        assertFalse(index.hasChildNode(n1));

        NodeBuilder node = index.getChildNode(START);
        assertEquals(":start pointing to wrong node", n0, getNext(node));

        node = index.getChildNode(n0);
        assertTrue("n0 should go nowhere", Strings.isNullOrEmpty(getNext(node)));

        // checking the first document
        String[] path = Iterables.toArray(PathUtils.elements(path0), String.class);
        node = node.getChildNode(path[0]);
        assertTrue(node.exists());
        node = node.getChildNode(path[1]);
        assertTrue(node.exists());
        assertTrue(node.getBoolean("match"));

        path = Iterables.toArray(PathUtils.elements(path0), String.class);
        node = index.getChildNode(n0).getChildNode(path[0]);
        assertTrue(node.exists());
        node = node.getChildNode(path[1]);
        assertTrue(node.exists());
        assertTrue(node.getBoolean("match"));

        // Stage 2
        store.update(index, path1, newHashSet(n0), newHashSet(n1));
        assertTrue(index.hasChildNode(START));
        assertTrue(index.hasChildNode(n0));
        assertTrue(index.hasChildNode(n1));

        node = index.getChildNode(START);
        assertEquals(":start pointing to wrong node", n0, getNext(node));

        node = index.getChildNode(n0);
        assertEquals(n1, getNext(node));
        path = Iterables.toArray(PathUtils.elements(path0), String.class);
        node = node.getChildNode(path[0]);
        assertTrue(node.exists());
        node = node.getChildNode(path[1]);
        assertTrue(node.exists());
        assertTrue(node.getBoolean("match"));
        path = Iterables.toArray(PathUtils.elements(path1), String.class);
        // we know both the documents share the same /content
        node = index.getChildNode(n0).getChildNode(path[0]);
        assertFalse("/content/two should no longer be under n0", node.hasChildNode(path[1]));

        node = index.getChildNode(n1);
        assertTrue("n1 should point nowhere", Strings.isNullOrEmpty(node.getString(NEXT)));
        path = Iterables.toArray(PathUtils.elements(path1), String.class);
        node = node.getChildNode(path[0]);
        assertTrue(node.exists());
        node = node.getChildNode(path[1]);
        assertTrue(node.exists());
        assertTrue(node.getBoolean("match"));
    }

    /**
     * test when a document is deleted and is the only one under the indexed key
     *
     * <p>
     * <i>it relies on the functionality of the store.update() for creating the
     * index</i>
     * </p>
     *
     * <code>
     *    Stage 1
     *    =======
     *
     *    :index : {
     *       :start : { :next = n0 },
     *       n0 : {
     *          :next = ,
     *          sampledoc : { match = true }
     *       }
     *    }
     *
     *    Stage 2
     *    =======
     *
     *    :index : {
     *       :start : { :next = }
     *    }
     * </code>
     */
    @Test
    public void deleteTheOnlyDocument() {
        final String n0 = KEYS[0];
        final String path = "/sampledoc";
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();

        // Stage 1 - initialising the index
        store.update(index, path, EMPTY_KEY_SET, newHashSet(n0));

        // we assume it works and therefore not checking the status of the index
        // let's go straight to Stage 2

        // Stage 2
        store.update(index, path, newHashSet(n0), EMPTY_KEY_SET);
        assertFalse("The node should have been removed", index.hasChildNode(n0));
        assertTrue("as the index should be empty, :start should point nowhere",
                        Strings.isNullOrEmpty(index.getChildNode(START).getString(NEXT)));
    }

    /**
     * test when the document is deleted but there're still some documents left
     * under the indexed key
     *
     * <p>
     * <i>it relies on the functionality of the store.update() for creating the
     * index</i>
     * </p>
     *
     * <code>
     *    Stage 1
     *    =======
     *
     *    :index : {
     *       :start : { :next = n0 },
     *       n0 : {
     *          :next = ,
     *          doc1 : { match=true },
     *          doc2 : { match=true }
     *       }
     *    }
     *
     *    Stage 2
     *    =======
     *
     *    :index : {
     *       :start : { :next = n0 },
     *       n0 : {
     *          :next  =,
     *          doc2 : { match = true }
     *       }
     *    }
     * </code>
     */
    @Test
    public void deleteOneOfTheDocuments() {
        final String n0 = KEYS[0];
        final String doc1 = "doc1";
        final String doc2 = "doc2";
        final String path1 = "/" + doc1;
        final String path2 = "/" + doc2;
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();

        store.update(index, path1, EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, path2, EMPTY_KEY_SET, newHashSet(n0));

        // we trust the store at this point and skip a double-check. Let's move
        // to Stage 2!

        store.update(index, path1, newHashSet(n0), EMPTY_KEY_SET);

        assertTrue(index.hasChildNode(START));
        assertTrue(index.hasChildNode(n0));
        assertEquals(":start should still point to n0", n0, getNext(index.getChildNode(START)));
        assertTrue("n0 should point nowhere", Strings.isNullOrEmpty(getNext(index.getChildNode(n0))));

        assertFalse(index.getChildNode(n0).hasChildNode(doc1));
        assertTrue(index.getChildNode(n0).hasChildNode(doc2));
        assertTrue(index.getChildNode(n0).getChildNode(doc2).getBoolean("match"));
    }

    /**
     * test when the only document is deleted from an indexed key but there're
     * still some keys left in the index
     *
     * <p>
     * <i>it relies on the functionality of the store.update() for creating the
     * index</i>
     * </p>
     *
     * <code>
     *    Stage 1
     *    =======
     *
     *    :index : {
     *       :start : { :next = n1 },
     *       n0 : {
     *          :next = ,
     *          content : {
     *             doc0 : { match = true }
     *          }
     *       },
     *       n1 : {
     *          :next = n2,
     *          content : {
     *             doc1 : { match = true }
     *          }
     *       }
     *       n2 : {
     *          :next = n0,
     *          content : {
     *             doc2 : { match = true }
     *          }
     *       }
     *    }
     *
     *    Stage 2
     *    =======
     *
     *    :index : {
     *       :start : { :next = n1 },
     *       n0 : {
     *          :next = ,
     *          content : {
     *             doc0 : { match = true }
     *          }
     *       },
     *       n1 : {
     *          :next = n0,
     *          content : {
     *             doc1 : { match = true }
     *          }
     *       }
     *    }
     *
     * </code>
     */
    @Test
    public void deleteTheOnlyDocumentInMultiKeysIndex() {
        final String path0 = "/content/doc0";
        final String path1 = "/content/doc1";
        final String path2 = "/content/doc2";
        final String n0 = KEYS[2];
        final String n1 = KEYS[0];
        final String n2 = KEYS[1];

        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();

        // Stage 1
        store.update(index, path0, EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, path1, EMPTY_KEY_SET, newHashSet(n1));
        store.update(index, path2, EMPTY_KEY_SET, newHashSet(n2));

        // as we trust the store we skip the check and goes straight to Stage 2.

        // Stage 2
        store.update(index, path2, newHashSet(n2), EMPTY_KEY_SET);

        // checking key nodes
        assertTrue(index.hasChildNode(START));
        assertTrue(index.hasChildNode(n0));
        assertTrue(index.hasChildNode(n1));
        assertFalse(index.hasChildNode(n2));

        // checking pointers
        assertEquals(n1, getNext(index.getChildNode(START)));
        assertEquals(n0, getNext(index.getChildNode(n1)));
        assertTrue(Strings.isNullOrEmpty(getNext(index.getChildNode(n0))));

        // checking sub-nodes
        String[] subNodes = Iterables.toArray(PathUtils.elements(path0), String.class);
        assertTrue(index.getChildNode(n0).hasChildNode(subNodes[0]));
        assertTrue(index.getChildNode(n0).getChildNode(subNodes[0]).hasChildNode(subNodes[1]));
        assertTrue(index.getChildNode(n0).getChildNode(subNodes[0]).getChildNode(subNodes[1]).getBoolean("match"));

        subNodes = Iterables.toArray(PathUtils.elements(path1), String.class);
        assertTrue(index.getChildNode(n1).hasChildNode(subNodes[0]));
        assertTrue(index.getChildNode(n1).getChildNode(subNodes[0]).hasChildNode(subNodes[1]));
        assertTrue(index.getChildNode(n1).getChildNode(subNodes[0]).getChildNode(subNodes[1]).getBoolean("match"));
    }

    /**
     * <p>test the insertion of 2 already ordered items</p>
     *
     * <p>expected</p>
     *
     *  <code>
     *      :index : {
     *          :start : { :next=n0 },
     *          n0 : { :next=n1 },
     *          n1 : { :next=}
     *      }
     *  </code>
     */
    @Test
    public void descendingOrderInsert2itemsAlreadyOrdered() {
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy(OrderDirection.DESC);
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n1 = KEYS[0];

        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        assertEquals(":start should point to the first node", n0,
            getNext(index.getChildNode(START)));
        assertEquals("n0 should point to n1", n1, getNext(index.getChildNode(n0)));
        assertTrue("n1 should point nowhere",
                   Strings.isNullOrEmpty(getNext(index.getChildNode(n1))));
    }

    /**
     * Tests the insert of 4 items that will always have to be added at the beginning of the list.
     * Just to simulate the use-case of lastModified DESC.
     *
     * <code>
     *      Stage 1
     *      =======
     *
     *      :index : {
     *          :start : { :next=n0 },
     *          n0 : { :next= }
     *      }
     *
     *      Stage 2
     *      =======
     *
     *      :index : {
     *          :start : { :next=n1 },
     *          n0 : { :next= },
     *          n1 : { :next=n0 }
     *      }
     *
     *      Stage 3
     *      =======
     *
     *      :index : {
     *          :start : { :next=n2 },
     *          n0 : { :next= },
     *          n1 : { :next=n0 },
     *          n2 : { :next=n1 }
     *      }
     *
     *      Stage 4
     *      =======
     *
     *      :index : {
     *          :start : { :next=n3 },
     *          n0 : { :next= },
     *          n1 : { :next=n0 },
     *          n2 : { :next=n1 },
     *          n3 : { :next=n2 }
     *      }
     * </code>
     */
    @Test
    public void descendingOrder4StagedInsertsAlwaysGreater() {
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy(OrderDirection.DESC);
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[0];
        String n1 = KEYS[1];
        String n2 = KEYS[2];
        String n3 = KEYS[3];

        // Stage 1
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        assertEquals(":start should point to n0", n0, getNext(index.getChildNode(START)));
        assertTrue("n0 should point nowhere",
                   Strings.isNullOrEmpty(getNext(index.getChildNode(n0))));

        // Stage 2
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        assertEquals(":start should point to n1", n1, getNext(index.getChildNode(START)));
        assertEquals("n1 should point to n0", n0, getNext(index.getChildNode(n1)));
        assertTrue("n0 should point nowhere",
                   Strings.isNullOrEmpty(getNext(index.getChildNode(n0))));

        // Stage 3
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        assertEquals(":start should point to n2", n2, getNext(index.getChildNode(START)));
        assertEquals("n2 should point to n1", n1, getNext(index.getChildNode(n2)));
        assertEquals("n1 should point to n0", n0, getNext(index.getChildNode(n1)));
        assertTrue("n0 should point nowhere",
                   Strings.isNullOrEmpty(getNext(index.getChildNode(n0))));

        // Stage 4
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));
        assertEquals(":start should point to n3", n3, getNext(index.getChildNode(START)));
        assertEquals("n3 should point to n2", n2, getNext(index.getChildNode(n3)));
        assertEquals("n2 should point to n1", n1, getNext(index.getChildNode(n2)));
        assertEquals("n1 should point to n0", n0, getNext(index.getChildNode(n1)));
        assertTrue("n0 should point nowhere",
                   Strings.isNullOrEmpty(getNext(index.getChildNode(n0))));
    }

    /**
     * test the insert of 1 item in a descending order index. it should not really matter but just
     * checking we don't break anything
     *
     * expecting
     *
     * <code>
     *  :index : {
     *      :start : { :next = n0 },
     *      n0 : { :next = }
     *  }
     * </code>
     */
    @Test
    public void descendingOrderInsert1item() {
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy(OrderDirection.DESC);
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];

        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        assertEquals(":start should point to the first node", n0,
            getNext(index.getChildNode(START)));
        assertTrue("the first node should point nowhere",
                   Strings.isNullOrEmpty(getNext(index.getChildNode(n0))));
    }

    /**
     * test the insert of 4 shuffled items in a descending ordered index
     *
     * expected:
     *
     * <code>
     *      :index : {
     *          :start : { :next= n1},
     *          n0 : { :next= n3},
     *          n1 : { :next= n2},
     *          n2: { :next= n0},
     *          n3 : { :next= },
     *      }
     * </code>
     */
    @Test
    public void descendingOrderInsert4ShuffledItems() {
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy(OrderDirection.DESC);
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n1 = KEYS[3];
        String n2 = KEYS[2];
        String n3 = KEYS[0];

        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));
        assertEquals(":start should point to n1", n1, getNext(index.getChildNode(START)));
        assertEquals("n0 should point to n3", n3, getNext(index.getChildNode(n0)));
        assertEquals("n1 should point to n2", n2, getNext(index.getChildNode(n1)));
        assertEquals("n2 should point to n1", n0, getNext(index.getChildNode(n2)));
        assertTrue("n3 should point nowhere",
                   Strings.isNullOrEmpty(getNext(index.getChildNode(n3))));
    }

    /**
     * test the iteration of the descending index with 2 shuffled items
     *
     * <code>
     *    :index : {
     *       :start : { :next=n1 },
     *       n0 : { :next= },
     *       n1 : { :next=n0 }
     *    }
     * </code>
     */
    @Test
    public void descendingOrderChildNodeEntriesACoupleOfMixedItems() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy(
                                                                                        OrderDirection.DESC);
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        final String n0 = KEYS[0];
        final String n1 = KEYS[1];

        // setting-up the index structure
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));

        NodeState indexState = index.getNodeState();
        NodeState node0 = indexState.getChildNode(n0);
        NodeState node1 = indexState.getChildNode(n1);

        Iterable<? extends ChildNodeEntry> children = store.getChildNodeEntries(indexState);
        assertNotNull("The iterable cannot be null", children);
        assertEquals("Expecting 2 items in the index", 2, Iterators.size(children.iterator()));

        // ensuring the right sequence
        ChildNodeEntry entry = null;
        children = store.getChildNodeEntries(indexState);
        Iterator<? extends ChildNodeEntry> it = children.iterator();
        assertTrue("We should have 2 elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The first element should be n1", n1, entry.getName());
        assertEquals("Wrong entry returned", node1, entry.getNodeState());
        assertTrue("We should have 1 elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The second element should be n0", n0, entry.getName());
        assertEquals("Wrong entry returned", node0, entry.getNodeState());
        assertFalse("We should have be at the end of the list", it.hasNext());
    }

    @Test
    public void count() throws IllegalArgumentException, RepositoryException {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        OrderedContentMirrorStoreStrategy descendingStore = new OrderedContentMirrorStoreStrategy(OrderDirection.DESC);
        final String orderedProperty = "fooprop";
        final String testAscendingName = "testascending";
        final String testDescendingName = "testdescending";
        final int numberOfNodes = 1000;
        final int maxNodeCount = 100;

        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();

        IndexUtils.createIndexDefinition(builder.child(IndexConstants.INDEX_DEFINITIONS_NAME),
            testAscendingName, false, ImmutableList.of(orderedProperty), null, OrderedIndex.TYPE,
            ImmutableMap.<String, String> of());
        IndexUtils.createIndexDefinition(builder.child(IndexConstants.INDEX_DEFINITIONS_NAME),
            testDescendingName, false, ImmutableList.of(orderedProperty), null, OrderedIndex.TYPE,
            ImmutableMap.<String, String> of(OrderedIndex.DIRECTION, OrderDirection.DESC.getDirection()));

        NodeBuilder ascendingContent = builder.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)
            .getChildNode(testAscendingName).child(IndexConstants.INDEX_CONTENT_NODE_NAME);
        NodeBuilder descendingContent = builder.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)
            .getChildNode(testDescendingName).child(IndexConstants.INDEX_CONTENT_NODE_NAME);

        // adding some content under the index
        for (int i = 0; i < numberOfNodes; i++) {
            store.update(ascendingContent, "/foo/bar", EMPTY_KEY_SET, newHashSet("x" + NF.format(i)));
            descendingStore.update(descendingContent, "/foo/bar", EMPTY_KEY_SET, newHashSet("x" + NF.format(i)));
        }

        assertEquals("wrong number of nodes encountered", numberOfNodes,
            Iterables.size(store.getChildNodeEntries(ascendingContent.getNodeState())));
        assertEquals("wrong number of nodes encountered", numberOfNodes,
            Iterables.size(descendingStore.getChildNodeEntries(descendingContent.getNodeState())));

        NodeState ascendingMeta = builder.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)
            .getChildNode(testAscendingName).getNodeState();
        NodeState descendingMeta = builder.getChildNode(IndexConstants.INDEX_DEFINITIONS_NAME)
            .getChildNode(testDescendingName).getNodeState();

        Filter.PropertyRestriction pr = null;

        // equality
        String value = "x" + NF.format(11);
        pr = new Filter.PropertyRestriction();
        pr.first = PropertyValues.newString(value);
        pr.last = PropertyValues.newString(value);
        pr.firstIncluding = true;
        pr.lastIncluding = true;
        assertEquals(1, store.count(ascendingMeta, pr, maxNodeCount));
        assertEquals(1, descendingStore.count(descendingMeta, pr, maxNodeCount));

        // property not null
        pr = new Filter.PropertyRestriction();
        pr.first = null;
        pr.last = null;
        pr.firstIncluding = false;
        pr.lastIncluding = false;
        // don't care about the actual results as long as we have something. We're reusing existing
        // code
        assertTrue(store.count(ascendingMeta, pr, maxNodeCount) > 0);
        assertEquals(store.count(ascendingMeta, pr, maxNodeCount),
            descendingStore.count(descendingMeta, pr, maxNodeCount));

        // '>'
        pr = new Filter.PropertyRestriction();
        pr.first = PropertyValues.newString(value);
        pr.last = null;
        pr.firstIncluding = false;
        pr.lastIncluding = false;
        // don't care about the actual results as long as we have something. We're reusing existing
        // code
        assertTrue(store.count(ascendingMeta, pr, maxNodeCount) > 0);
        assertTrue(descendingStore.count(ascendingMeta, pr, maxNodeCount) > 0);

        // '>='
        pr = new Filter.PropertyRestriction();
        pr.first = PropertyValues.newString(value);
        pr.last = null;
        pr.firstIncluding = true;
        pr.lastIncluding = false;
        // don't care about the actual results as long as we have something. We're reusing existing
        // code
        assertTrue(store.count(ascendingMeta, pr, maxNodeCount) > 0);
        assertTrue(descendingStore.count(ascendingMeta, pr, maxNodeCount) > 0);

        // '<'
        pr = new Filter.PropertyRestriction();
        pr.first = null;
        pr.last = PropertyValues.newString(value);
        pr.firstIncluding = false;
        pr.lastIncluding = false;
        // don't care about the actual results as long as we have something. We're reusing existing
        // code
        assertTrue(descendingStore.count(descendingMeta, pr, maxNodeCount) > 0);
        assertTrue(store.count(ascendingMeta, pr, maxNodeCount) > 0);

        // when no conditions has been asked but just an ORDER BY
        pr = null;
        assertTrue(store.count(ascendingMeta, pr, maxNodeCount) > 0);
        assertEquals(store.count(ascendingMeta, pr, maxNodeCount),
            descendingStore.count(descendingMeta, pr, maxNodeCount));
    }
    
    @Test
    public void seekEqualsNotFound() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n1 = KEYS[3];
        String n2 = KEYS[2];
        String n3 = KEYS[0];
        String nonExisting = "dsrfgdrtfhg";

        // initialising the store
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));

        assertNull("The item should have not been found", store.seek(
            index.getNodeState(),
            new OrderedContentMirrorStoreStrategy.PredicateEquals(nonExisting)));
    }

    @Test
    public void seekEquals() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n1 = KEYS[3];
        String n2 = KEYS[2];
        String n3 = KEYS[0];

        // initialising the store
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));

        String searchFor = n1;

        ChildNodeEntry item = store.seek(index.getNodeState(),
            new OrderedContentMirrorStoreStrategy.PredicateEquals(searchFor));

        assertNotNull("we should have found an item", item);
        assertEquals(searchFor, item.getName());
    }

    @Test
    public void seekGreaterThanNotFound() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n1 = KEYS[3];
        String n2 = KEYS[2];
        String n3 = KEYS[0];

        // initialising the store
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));

        String searchFor = n1;

        ChildNodeEntry item = store.seek(index.getNodeState(),
            new OrderedContentMirrorStoreStrategy.PredicateGreaterThan(searchFor));

        assertNull("no item should have been found", item);
    }

    @Test
    public void seekGreaterThan() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n1 = KEYS[3];
        String n2 = KEYS[2];
        String n3 = KEYS[0];

        // initialising the store
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));

        printSkipList(index.getNodeState());
        
        String searchFor = n2;

        ChildNodeEntry item = store.seek(index.getNodeState(),
            new OrderedContentMirrorStoreStrategy.PredicateGreaterThan(searchFor));

        assertNotNull("we should have found an item", item);
        assertEquals(n1, item.getName());
    }

    @Test
    public void seekGreaterThanEqualsNotFound() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n2 = KEYS[2];
        String n3 = KEYS[0];

        // initialising the store
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));

        String searchFor = KEYS[3];

        ChildNodeEntry item = store.seek(index.getNodeState(),
            new OrderedContentMirrorStoreStrategy.PredicateGreaterThan(searchFor, true));

        assertNull("we should have not found an item", item);
    }

    @Test
    public void seekGreaterThanEquals() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n2 = KEYS[2];
        String n3 = KEYS[0];

        // initialising the store
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));

        String searchFor = n2;

        ChildNodeEntry item = store.seek(index.getNodeState(),
            new OrderedContentMirrorStoreStrategy.PredicateGreaterThan(searchFor, true));

        assertNotNull("we should have found an item", item);
        assertEquals(n2, item.getName());
    }

    @Test
    public void seekLessThanNotFound() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy(
            OrderDirection.DESC);
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n2 = KEYS[2];
        String n3 = KEYS[0];

        // initialising the store
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));

        String searchFor = n3;

        ChildNodeEntry item = store.seek(index.getNodeState(),
            new OrderedContentMirrorStoreStrategy.PredicateLessThan(searchFor));

        assertNull("we should have not found an item", item);
    }

    @Test
    public void seekLessThan() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy(
            OrderDirection.DESC);
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n2 = KEYS[2];
        String n3 = KEYS[0];

        // initialising the store
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));

        String searchFor = n2;

        ChildNodeEntry item = store.seek(index.getNodeState(),
            new OrderedContentMirrorStoreStrategy.PredicateLessThan(searchFor));

        assertNotNull("we should have found an item", item);
        assertEquals(n0, item.getName());
    }

    @Test
    public void seekLessThanEqualNotFound() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy(
            OrderDirection.DESC);
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n1 = KEYS[3];
        String n2 = KEYS[2];

        // initialising the store
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));

        String searchFor = KEYS[0];

        ChildNodeEntry item = store.seek(index.getNodeState(),
            new OrderedContentMirrorStoreStrategy.PredicateLessThan(searchFor, true));

        assertNull("we should have not found an item", item);
    }

    @Test
    public void seekLessThanEqual() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy(
            OrderDirection.DESC);
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n1 = KEYS[3];
        String n2 = KEYS[2];

        // initialising the store
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));

        String searchFor = n2;

        ChildNodeEntry item = store.seek(index.getNodeState(),
            new OrderedContentMirrorStoreStrategy.PredicateLessThan(searchFor, true));

        assertNotNull("we should have found an item", item);
        assertEquals(n2, item.getName());
    }
    
    private static String getNext(@Nonnull NodeBuilder node) {
        return getNext(node.getNodeState());
    }
    
    private static String getNext(@Nonnull NodeState node) {
        return Iterables.toArray(node.getProperty(NEXT).getValue(Type.STRINGS), String.class)[0];
    }

    private static String getNext(@Nonnull NodeState node, int lane) {
        return Iterables.toArray(node.getProperty(NEXT).getValue(Type.STRINGS), String.class)[lane];
    }

    private static Iterable<String> getMultiNext(@Nonnull NodeState node) {
        return node.getProperty(NEXT).getValue(Type.STRINGS);
    }
    
    @Test
    public void setNext() {
        NodeBuilder n = EmptyNodeState.EMPTY_NODE.builder();
        
        OrderedContentMirrorStoreStrategy.setPropertyNext(n, "foobar");
        assertNotNull(n);
        assertNotNull(":next cannot be null", n.getProperty(NEXT));
        assertEquals(ImmutableList.of("foobar", "", "", ""), 
            n.getProperty(NEXT).getValue(Type.STRINGS));
        
        OrderedContentMirrorStoreStrategy.setPropertyNext(n, (String[]) null);
        assertNotNull(n);
        assertNotNull(":next cannot be null", n.getProperty(NEXT));
        assertEquals("If I set a value to null, nothing should change",
            ImmutableList.of("foobar", "", "", ""), n.getProperty(NEXT).getValue(Type.STRINGS));

        OrderedContentMirrorStoreStrategy.setPropertyNext(n, "");
        assertNotNull(n);
        assertNotNull(":next cannot be null", n.getProperty(NEXT));
        assertEquals(ImmutableList.of("", "", "", ""), 
            n.getProperty(NEXT).getValue(Type.STRINGS));
        
        n = EmptyNodeState.EMPTY_NODE.builder();
        OrderedContentMirrorStoreStrategy.setPropertyNext(n, "a", "b");
        assertNotNull(n);
        assertNotNull(":next cannot be null", n.getProperty(NEXT));
        assertEquals(ImmutableList.of("a", "b", "", ""), 
            n.getProperty(NEXT).getValue(Type.STRINGS));

        n = EmptyNodeState.EMPTY_NODE.builder();
        OrderedContentMirrorStoreStrategy.setPropertyNext(n, "a", "b", "c");
        assertNotNull(n);
        assertNotNull(":next cannot be null", n.getProperty(NEXT));
        assertEquals(ImmutableList.of("a", "b", "c", ""), 
            n.getProperty(NEXT).getValue(Type.STRINGS));

        n = EmptyNodeState.EMPTY_NODE.builder();
        OrderedContentMirrorStoreStrategy.setPropertyNext(n, "a", "b", "c", "d");
        assertNotNull(n);
        assertNotNull(":next cannot be null", n.getProperty(NEXT));
        assertEquals(ImmutableList.of("a", "b", "c", "d"), 
            n.getProperty(NEXT).getValue(Type.STRINGS));

        n = EmptyNodeState.EMPTY_NODE.builder();
        OrderedContentMirrorStoreStrategy.setPropertyNext(n, "a", "b", "c", "d", "e", "f");
        assertNotNull(n);
        assertNotNull(":next cannot be null", n.getProperty(NEXT));
        assertEquals("even if we provide more than 4 nexts we expect it to take only the first 4s", 
            ImmutableList.of("a", "b", "c", "d"), 
            n.getProperty(NEXT).getValue(Type.STRINGS));
        
        n = EmptyNodeState.EMPTY_NODE.builder();
        n.setProperty(NEXT, ImmutableList.of("a", "b", "c", "d"), Type.STRINGS);
        OrderedContentMirrorStoreStrategy.setPropertyNext(n, "a", 3);
        assertNotNull(n);
        assertNotNull(":next cannot be null", n.getProperty(NEXT));
        assertEquals(ImmutableList.of("a", "b", "c", "a"),
            n.getProperty(NEXT).getValue(Type.STRINGS));
    }
    
    @Test
    public void getNext() {
        NodeBuilder node = EmptyNodeState.EMPTY_NODE.builder();
        assertEquals("If the property is not there an empty string is expected", "",
            OrderedContentMirrorStoreStrategy.getPropertyNext(node.getNodeState()));
        
        node.setProperty(NEXT, ImmutableList.of("bar", "", "", ""), Type.STRINGS);
        assertEquals("bar", OrderedContentMirrorStoreStrategy.getPropertyNext(node.getNodeState()));

        node.setProperty(NEXT, ImmutableList.of("", "", "", ""), Type.STRINGS);
        assertEquals("", OrderedContentMirrorStoreStrategy.getPropertyNext(node.getNodeState()));
        
        node = EmptyNodeState.EMPTY_NODE.builder();
        assertEquals("If the property is not there an empty string is expected", "",
            OrderedContentMirrorStoreStrategy.getPropertyNext(node));
        
        node.setProperty(NEXT, ImmutableList.of("bar", "", "", ""), Type.STRINGS);
        assertEquals("bar", OrderedContentMirrorStoreStrategy.getPropertyNext(node));

        node.setProperty(NEXT, ImmutableList.of("", "", "", ""), Type.STRINGS);
        assertEquals("", OrderedContentMirrorStoreStrategy.getPropertyNext(node));
        
        node.setProperty(NEXT, ImmutableList.of("a", "b", "c", "d"), Type.STRINGS);
        assertEquals("a", OrderedContentMirrorStoreStrategy.getPropertyNext(node));
        assertEquals("a", OrderedContentMirrorStoreStrategy.getPropertyNext(node, 0));
        assertEquals("b", OrderedContentMirrorStoreStrategy.getPropertyNext(node, 1));
        assertEquals("c", OrderedContentMirrorStoreStrategy.getPropertyNext(node, 2));
        assertEquals("d", OrderedContentMirrorStoreStrategy.getPropertyNext(node, 3));
        assertEquals("the highest available lane is expected", "d",
            OrderedContentMirrorStoreStrategy.getPropertyNext(node, OrderedIndex.LANES + 100));
        
        node.setProperty(NEXT, "a", Type.STRING);
        assertEquals("a", OrderedContentMirrorStoreStrategy.getPropertyNext(node));
        assertEquals("a", OrderedContentMirrorStoreStrategy.getPropertyNext(node, 0));
        assertEquals("a", OrderedContentMirrorStoreStrategy.getPropertyNext(node, 1));
        assertEquals("a", OrderedContentMirrorStoreStrategy.getPropertyNext(node, 2));
        assertEquals("a", OrderedContentMirrorStoreStrategy.getPropertyNext(node, 3));

        node.setProperty(NEXT, ImmutableList.of("a", "b"), Type.STRINGS);
        assertEquals("a", OrderedContentMirrorStoreStrategy.getPropertyNext(node));
        assertEquals("a", OrderedContentMirrorStoreStrategy.getPropertyNext(node, 0));
        assertEquals("b", OrderedContentMirrorStoreStrategy.getPropertyNext(node, 1));
        assertEquals("b", OrderedContentMirrorStoreStrategy.getPropertyNext(node, 2));
        assertEquals("b", OrderedContentMirrorStoreStrategy.getPropertyNext(node, 3));
    }
    
    @Test
    public void getLane() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        Random generator = null;
        
        // Default probability is 0.1
        
        generator = createNiceMock(Random.class);
        expect(generator.nextDouble()).andReturn(0.73).anyTimes();
        replay(generator);        
        assertEquals(0, store.getLane(generator));
        
        generator = createNiceMock(Random.class);
        expect(generator.nextDouble()).andReturn(0.02).once();
        expect(generator.nextDouble()).andReturn(0.73).once();
        replay(generator);
        assertEquals(1, store.getLane(generator));

        generator = createNiceMock(Random.class);
        expect(generator.nextDouble()).andReturn(0.02).times(2);
        expect(generator.nextDouble()).andReturn(0.73).once();
        replay(generator);
        assertEquals(2, store.getLane(generator));

        generator = createNiceMock(Random.class);
        expect(generator.nextDouble()).andReturn(0.02).times(3);
        expect(generator.nextDouble()).andReturn(0.73).once();
        replay(generator);
        assertEquals(3, store.getLane(generator));

        generator = createNiceMock(Random.class);
        expect(generator.nextDouble()).andReturn(0.02).times(OrderedIndex.LANES);
        expect(generator.nextDouble()).andReturn(0.73).once();
        replay(generator);
        assertEquals("we should never go beyond 4 lanes", OrderedIndex.LANES - 1,
            store.getLane(generator));
    }
    
    /**
     * Test the insert of 1 item into an empty index. Start should always point with all the lanes
     * to the first element
     * 
     */
    @Test
    public void insertWithLanes1Item() {
        MockOrderedContentMirrorStoreStrategy store = new MockOrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[0];
        
       
        /*
         * with lane==0
         * 
         *  :index : {
         *      :start : { :next = [n0, , , ] },
         *      n0 : { :next = [ , , , ] }
         *  }
         */
        store.setLane(0);
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        
        NodeBuilder n = index.getChildNode(START);
        assertNotNull("There should always be a :start", n);
        assertEquals(":start's :next should always point to the first element", 
            ImmutableList.of(n0, "", "", ""),
            n.getProperty(NEXT).getValue(Type.STRINGS)
        );
        n = index.getChildNode(n0);
        assertNotNull(n);
        assertEquals(OrderedContentMirrorStoreStrategy.EMPTY_NEXT,
            n.getProperty(NEXT).getValue(Type.STRINGS)
        );
        
        /*
         * with lane==1
         * 
         *  :index : {
         *      :start : { :next = [n0, n0, , ] },
         *      n0 : { :next = [ , , , ] }
         *  }
         */
        index = EmptyNodeState.EMPTY_NODE.builder();
        store.setLane(1);
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        
        n = index.getChildNode(START);
        assertNotNull("There should always be a :start", n);
        assertEquals(":start's :next should always point to the first element", 
            ImmutableList.of(n0, n0, "", ""),
            n.getProperty(NEXT).getValue(Type.STRINGS)
        );
        n = index.getChildNode(n0);
        assertNotNull(n);
        assertEquals(OrderedContentMirrorStoreStrategy.EMPTY_NEXT,
            n.getProperty(NEXT).getValue(Type.STRINGS)
        );

        /*
         * with lane==2
         * 
         *  :index : {
         *      :start : { :next = [n0, n0, n0, ] },
         *      n0 : { :next = [ , , , ] }
         *  }
         */
        index = EmptyNodeState.EMPTY_NODE.builder();
        store.setLane(2);
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        
        n = index.getChildNode(START);
        assertNotNull("There should always be a :start", n);
        assertEquals(":start's :next should always point to the first element", 
            ImmutableList.of(n0, n0, n0, ""),
            n.getProperty(NEXT).getValue(Type.STRINGS)
        );
        n = index.getChildNode(n0);
        assertNotNull(n);
        assertEquals(OrderedContentMirrorStoreStrategy.EMPTY_NEXT,
            n.getProperty(NEXT).getValue(Type.STRINGS)
        );
        
        /*
         * with lane==3
         * 
         *  :index : {
         *      :start : { :next = [n0, n0, n0, n0 ] },
         *      n0 : { :next = [ , , , ] }
         *  }
         */
        index = EmptyNodeState.EMPTY_NODE.builder();
        store.setLane(3);
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        
        n = index.getChildNode(START);
        assertNotNull("There should always be a :start", n);
        assertEquals(":start's :next should always point to the first element", 
            ImmutableList.of(n0, n0, n0, n0),
            n.getProperty(NEXT).getValue(Type.STRINGS)
        );
        n = index.getChildNode(n0);
        assertNotNull(n);
        assertEquals(OrderedContentMirrorStoreStrategy.EMPTY_NEXT,
            n.getProperty(NEXT).getValue(Type.STRINGS)
        );
    }
    

    /**
     * tests the insert of an item that has to be appended 
     */
    @Test 
    public void laneInsert2ItemsAlreadyOrdere() {
        MockOrderedContentMirrorStoreStrategy store = new MockOrderedContentMirrorStoreStrategy();
        NodeBuilder index = null;
        NodeBuilder n = null;
        String n0 = KEYS[0];
        String n1 = KEYS[1];
        
        // this one should be covered by insertWithLanes1Item(). Not testing
        
        /* 
         * if lane is 0 we're expecting the following
         * 
         *  :index : {
         *      :start  : { :next : [n0, , , ] },
         *      n0      : { :next : [n1, , , ] }
         *      n1      : { :next : [ , , , ] }
         *  }
         */
        index = EmptyNodeState.EMPTY_NODE.builder();
        store.setLane(0);
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        printSkipList(index.getNodeState());
        n = index.getChildNode(START); 
        assertNotNull(n);
        assertNotNull(n.getProperty(NEXT));
        assertEquals(ImmutableList.of(n0, "", "", ""), n.getProperty(NEXT).getValue(Type.STRINGS));
        
        n = index.getChildNode(n0); 
        assertNotNull(n);
        assertNotNull(n.getProperty(NEXT));
        assertEquals(ImmutableList.of(n1, "", "", ""), n.getProperty(NEXT).getValue(Type.STRINGS));
        
        n = index.getChildNode(n1); 
        assertNotNull(n);
        assertNotNull(n.getProperty(NEXT));
        assertEquals(OrderedContentMirrorStoreStrategy.EMPTY_NEXT,
            n.getProperty(NEXT).getValue(Type.STRINGS));

        /* 
         * if lane == 1 on n1 insert
         * 
         *  :index : {
         *      :start  : { :next : [n0, n1, , ] },
         *      n0      : { :next : [n1, , , ] }
         *      n1      : { :next : [ , , , ] }
         *  }
         */
        index = EmptyNodeState.EMPTY_NODE.builder();
        store.setLane(0);
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.setLane(1);
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        printSkipList(index.getNodeState());
        n = index.getChildNode(START); 
        assertNotNull(n);
        assertNotNull(n.getProperty(NEXT));
        assertEquals(ImmutableList.of(n0, n1, "", ""), n.getProperty(NEXT).getValue(Type.STRINGS));
        
        n = index.getChildNode(n0); 
        assertNotNull(n);
        assertNotNull(n.getProperty(NEXT));
        assertEquals(ImmutableList.of(n1, "", "", ""), n.getProperty(NEXT).getValue(Type.STRINGS));
        
        n = index.getChildNode(n1); 
        assertNotNull(n);
        assertNotNull(n.getProperty(NEXT));
        assertEquals(OrderedContentMirrorStoreStrategy.EMPTY_NEXT,
            n.getProperty(NEXT).getValue(Type.STRINGS));
    }
    
    /**
     * Testing The Insert Of Shuffled Items And Lanes Recreating The Following Index Structure
     * 
     *  <Code>
     *      List Structure
     *      ==============
     *      (Above The Node Names Is The Insert Order)
     *      
     *              9   5   6   4   7   1   0   3   10  2   12  11  8
     *         -----------------------------------------------------------
     *         Str 000 001 002 003 004 005 006 007 008 009 010 011 012 Nil
     *          |-->o-->o-->o-->o-->o-->o-->o-->o-->o-->o-->o-->o-->o-->|
     *          |------>o------>o------>o------>o---------->o---------->|
     *          |-------------->o-------------->o---------->o---------->|
     *          |------------------------------------------>o---------->|
     *  </Code>
     */
    @Test
    public void insertShuffledItemsWithLanes() {
        MockOrderedContentMirrorStoreStrategy ascStore = new MockOrderedContentMirrorStoreStrategy();
        MockOrderedContentMirrorStoreStrategy descStore = new MockOrderedContentMirrorStoreStrategy(
            OrderDirection.DESC);
        NodeBuilder ascIndex = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder descIndex = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder node;
        NodeBuilder index;
        String n00 = KEYS[0];
        String n01 = KEYS[1];
        String n02 = KEYS[2];
        String n03 = KEYS[3];
        String n04 = KEYS[4];
        String n05 = KEYS[5];
        String n06 = KEYS[6];
        String n07 = KEYS[7];
        String n08 = KEYS[8];
        String n09 = KEYS[9];
        String n10 = KEYS[10];
        String n11 = KEYS[11];
        String n12 = KEYS[12];
        
        /*
         * Stage 0
         */
        ascStore.setLane(0);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n06));
        descStore.setLane(0);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n06));
        node = ascIndex.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = ascIndex.getChildNode(n06);
        assertNotNull(node);
        assertEquals(OrderedContentMirrorStoreStrategy.EMPTY_NEXT,
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = descIndex.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = descIndex.getChildNode(n06);
        assertNotNull(node);
        assertEquals(OrderedContentMirrorStoreStrategy.EMPTY_NEXT,
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        /*
         * Stage 1
         */
        ascStore.setLane(1);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n05));
        descStore.setLane(1);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n05));

        index = ascIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, n05, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(OrderedContentMirrorStoreStrategy.EMPTY_NEXT,
            node.getProperty(NEXT).getValue(Type.STRINGS));

        index = descIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n05, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(OrderedContentMirrorStoreStrategy.EMPTY_NEXT,
            node.getProperty(NEXT).getValue(Type.STRINGS));

        /*
         * Stage 2
         */
        ascStore.setLane(0);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n09));
        descStore.setLane(0);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n09));
        
        index = ascIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, n05, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        index = descIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, n05, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(OrderedContentMirrorStoreStrategy.EMPTY_NEXT,
            node.getProperty(NEXT).getValue(Type.STRINGS));

        /*
         * Stage 3
         */
        ascStore.setLane(2);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n07));
        descStore.setLane(2);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n07));

        node = ascIndex.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, n05, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = ascIndex.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n07, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = ascIndex.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = ascIndex.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = ascIndex.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        index = descIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, n07, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n05, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(OrderedContentMirrorStoreStrategy.EMPTY_NEXT,
            node.getProperty(NEXT).getValue(Type.STRINGS));

        /*
         * Stage 4
         */
        ascStore.setLane(2);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n03));

        descStore.setLane(2);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n03));

        node = ascIndex.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, n03, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, n05, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n07, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        index = descIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, n07, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n05, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        /*
         * Stage 5
         */
        ascStore.setLane(1);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n01));
        descStore.setLane(1);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n01));

        node = ascIndex.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, n01, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, n05, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n07, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        index = descIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, n07, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n05, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, n01, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        /*
         * Stage 6
         */
        ascStore.setLane(0);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n02));
        descStore.setLane(0);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n02));

        node = ascIndex.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, n01, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, n05, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n07, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        index = descIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, n07, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n05, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n01, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        /*
         * Stage 7
         */
        ascStore.setLane(0);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n04));
        descStore.setLane(0);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n04));

        node = ascIndex.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, n01, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n04, n05, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n04);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n07, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        index = descIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, n07, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n05, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n04, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n04);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n01, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        /*
         * Stage 8
         */
        ascStore.setLane(0);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n12));
        descStore.setLane(0);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n12));

        node = ascIndex.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, n01, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n04, n05, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n04);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n07, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n12, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n12);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        index = descIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n12, n07, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n12);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n05, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n04, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n04);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n01, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        /*
         * Stage 9
         */
        ascStore.setLane(0);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n00));
        descStore.setLane(0);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n00));

        node = ascIndex.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n00, n01, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n00);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n04, n05, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n04);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n07, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n12, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n12);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        index = descIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n12, n07, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n12);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n05, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n04, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n04);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n01, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n00, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n00);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        /**
         * Stage 10
         */
        ascStore.setLane(0);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n08));
        descStore.setLane(0);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n08));

        node = ascIndex.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n00, n01, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n00);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n04, n05, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n04);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n07, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n08, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n08);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n12, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n12);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        index = descIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n12, n07, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n12);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n08, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n08);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n05, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n04, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n04);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n01, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n00, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n00);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        /**
         * Stage 11
         */
        ascStore.setLane(0);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n11));
        descStore.setLane(0);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n11));

        node = ascIndex.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n00, n01, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n00);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n04, n05, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n04);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n07, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n08, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n08);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n11, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n11);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n12, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n12);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        index = descIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n12, n07, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n12);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n11, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n11);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n08, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n08);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n05, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n04, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n04);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n01, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n00, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n00);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        /*
         * Stage 12
         */
        ascStore.setLane(3);
        ascStore.update(ascIndex, "/a", EMPTY_KEY_SET, newHashSet(n10));
        descStore.setLane(3);
        descStore.update(descIndex, "/a", EMPTY_KEY_SET, newHashSet(n10));

        node = ascIndex.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n00, n01, n03, n10),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n00);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n04, n05, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n04);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n07, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n08, n10, n10, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n08);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        
        node = ascIndex.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n10, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n10);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n11, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n11);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n12, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        node = ascIndex.getChildNode(n12);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));

        index = descIndex;
        node = index.getChildNode(START);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n12, n10, n10, n10),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n12);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n11, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n11);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n10, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n10);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n09, n07, n07, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n09);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n08, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n08);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n07, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n07);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n06, n05, n03, ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n06);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n05, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n05);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n04, n03, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n04);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n03, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n03);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n02, n01, "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n02);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n01, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n01);
        assertNotNull(node);
        assertEquals(ImmutableList.of(n00, "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n00);
        assertNotNull(node);
        assertEquals(ImmutableList.of("", "", "", ""),
            node.getProperty(NEXT).getValue(Type.STRINGS));
    }
    
    /**
     * testing the seek method and the returned lanes with the following index structure
     * 
     *  <code>
     *      List Structure
     *      ==============
     *      
     *         STR 000 001 002 003 004 005 006 007 008 009 010 011 012 NIL
     *          |-->o-->o-->o-->o-->o-->o-->o-->o-->o-->o-->o-->o-->o-->|
     *          |------>o------>o------>o------>o---------->o---------->|
     *          |-------------->o-------------->o---------->o---------->|
     *          |------------------------------------------>o---------->|
     *  </code>
     */
    @Test
    public void seekEqualsWithLanes() {
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        String n00 = KEYS[0];
        String n01 = KEYS[1];
        String n02 = KEYS[2];
        String n03 = KEYS[3];
        String n04 = KEYS[4];
        String n05 = KEYS[5];
        String n06 = KEYS[6];
        String n07 = KEYS[7];
        String n08 = KEYS[8];
        String n09 = KEYS[9];
        String n10 = KEYS[10];
        String n11 = KEYS[11];
        String n12 = KEYS[12];

        // initialising the store
        builder.child(START).setProperty(NEXT, ImmutableList.of(n00, n01, n03, n10), Type.STRINGS);
        builder.child(n00).setProperty(NEXT,   ImmutableList.of(n01,  "",  "",  ""), Type.STRINGS);
        builder.child(n01).setProperty(NEXT,   ImmutableList.of(n02, n03,  "",  ""), Type.STRINGS);
        builder.child(n02).setProperty(NEXT,   ImmutableList.of(n03,  "",  "",  ""), Type.STRINGS);
        builder.child(n03).setProperty(NEXT,   ImmutableList.of(n04, n05, n07,  ""), Type.STRINGS);
        builder.child(n04).setProperty(NEXT,   ImmutableList.of(n05,  "",  "",  ""), Type.STRINGS);
        builder.child(n05).setProperty(NEXT,   ImmutableList.of(n06, n07,  "",  ""), Type.STRINGS);
        builder.child(n06).setProperty(NEXT,   ImmutableList.of(n07,  "",  "",  ""), Type.STRINGS);
        builder.child(n07).setProperty(NEXT,   ImmutableList.of(n08, n10, n10,  ""), Type.STRINGS);
        builder.child(n08).setProperty(NEXT,   ImmutableList.of(n09,  "",  "",  ""), Type.STRINGS);
        builder.child(n09).setProperty(NEXT,   ImmutableList.of(n10, n12,  "",  ""), Type.STRINGS);
        builder.child(n10).setProperty(NEXT,   ImmutableList.of(n11,  "",  "",  ""), Type.STRINGS);
        builder.child(n11).setProperty(NEXT,   ImmutableList.of(n12,  "",  "",  ""), Type.STRINGS);
        builder.child(n12).setProperty(NEXT,   ImmutableList.of("" ,  "",  "",  ""), Type.STRINGS);

        NodeState index = builder.getNodeState();
        
        printSkipList(index);

        // testing the exception in case of wrong parameters
        String searchFor = "wedontcareaswetesttheexception";
        NodeState node = index.getChildNode(searchFor);
        ChildNodeEntry entry = new OrderedChildNodeEntry(
            searchFor, node);
        ChildNodeEntry[] wl = new ChildNodeEntry[0];
        ChildNodeEntry item = null;
        ChildNodeEntry lane0, lane1, lane2, lane3;
        
        try {
            item = store.seek(index,
                new OrderedContentMirrorStoreStrategy.PredicateEquals(searchFor), wl);
            fail("With a wrong size for the lane it should have raised an exception");
        } catch (IllegalArgumentException e) {
            // so far so good. It was expected
        }
        
        // testing equality
        searchFor = n12;
        lane3 = new OrderedChildNodeEntry(n10, index.getChildNode(n10));
        lane2 = new OrderedChildNodeEntry(n10, index.getChildNode(n10));
        lane1 = new OrderedChildNodeEntry(n10, index.getChildNode(n10));
        lane0 = new OrderedChildNodeEntry(n11, index.getChildNode(n11));
        entry = new OrderedChildNodeEntry(searchFor,
            index.getChildNode(searchFor));
        wl = new ChildNodeEntry[OrderedIndex.LANES];
        item = store.seek(index,
            new OrderedContentMirrorStoreStrategy.PredicateEquals(searchFor), wl);
        assertNotNull(wl);
        assertEquals(OrderedIndex.LANES, wl.length);
        assertEquals("Wrong lane", lane0, wl[0]);
        assertEquals("Wrong lane", lane1, wl[1]);
        assertEquals("Wrong lane", lane2, wl[2]);
        assertEquals("Wrong lane", lane3, wl[3]);
        assertEquals("Wrong item returned", entry, item);

        searchFor = n08;
        lane3 = new OrderedChildNodeEntry(START, index.getChildNode(START));
        lane2 = new OrderedChildNodeEntry(n07, index.getChildNode(n07));
        lane1 = new OrderedChildNodeEntry(n07, index.getChildNode(n07));
        lane0 = new OrderedChildNodeEntry(n07, index.getChildNode(n07));
        entry = new OrderedChildNodeEntry(searchFor,
            index.getChildNode(searchFor));
        wl = new ChildNodeEntry[OrderedIndex.LANES];
        item = store.seek(index,
            new OrderedContentMirrorStoreStrategy.PredicateEquals(searchFor), wl);
        assertNotNull(wl);
        assertEquals(OrderedIndex.LANES, wl.length);
        assertEquals("Wrong lane", lane0, wl[0]);
        assertEquals("Wrong lane", lane1, wl[1]);
        assertEquals("Wrong lane", lane2, wl[2]);
        assertEquals("Wrong lane", lane3, wl[3]);
        assertEquals("Wrong item returned", entry, item);

        searchFor = n06;
        lane3 = new OrderedChildNodeEntry(START, index.getChildNode(START));
        lane2 = new OrderedChildNodeEntry(n03, index.getChildNode(n03));
        lane1 = new OrderedChildNodeEntry(n05, index.getChildNode(n05));
        lane0 = new OrderedChildNodeEntry(n05, index.getChildNode(n05));
        entry = new OrderedChildNodeEntry(searchFor,
            index.getChildNode(searchFor));
        wl = new ChildNodeEntry[OrderedIndex.LANES];
        item = store.seek(index,
            new OrderedContentMirrorStoreStrategy.PredicateEquals(searchFor), wl);
        assertNotNull(wl);
        assertEquals(OrderedIndex.LANES, wl.length);
        assertEquals("Wrong lane", lane0, wl[0]);
        assertEquals("Wrong lane", lane1, wl[1]);
        assertEquals("Wrong lane", lane2, wl[2]);
        assertEquals("Wrong lane", lane3, wl[3]);
        assertEquals("Wrong item returned", entry, item);
    }

    /**
     * testing the seek method and the returned lanes with the following index structure
     * 
     *  <code>
     *      List Structure
     *      ==============
     *      
     *         STR 012 011 010 009 008 007 006 005 004 003 002 001 000 NIL
     *          |-->o-->o-->o-->o-->o-->o-->o-->o-->o-->o-->o-->o-->o-->|
     *          |------>o------>o------>o------>o---------->o---------->|
     *          |-------------->o-------------->o---------->o---------->|
     *          |------------------------------------------>o---------->|
     *  </code>
     */
    @Test
    public void seekEqualsWithLanesDescending() {
        // testing the walking lanes with a descending order index
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy(
            OrderDirection.DESC);
        NodeBuilder builder = EmptyNodeState.EMPTY_NODE.builder();
        String n00 = KEYS[0];
        String n01 = KEYS[1];
        String n02 = KEYS[2];
        String n03 = KEYS[3];
        String n04 = KEYS[4];
        String n05 = KEYS[5];
        String n06 = KEYS[6];
        String n07 = KEYS[7];
        String n08 = KEYS[8];
        String n09 = KEYS[9];
        String n10 = KEYS[10];
        String n11 = KEYS[11];
        String n12 = KEYS[12];

        // initialising the store
        builder.child(START).setProperty(NEXT, ImmutableList.of(n12, n11, n09, n02), Type.STRINGS);
        builder.child(n12).setProperty(NEXT,   ImmutableList.of(n11 , "",  "",  ""), Type.STRINGS);
        builder.child(n11).setProperty(NEXT,   ImmutableList.of(n10, n09,  "",  ""), Type.STRINGS);
        builder.child(n10).setProperty(NEXT,   ImmutableList.of(n09,  "",  "",  ""), Type.STRINGS);
        builder.child(n09).setProperty(NEXT,   ImmutableList.of(n08, n07, n05,  ""), Type.STRINGS);
        builder.child(n08).setProperty(NEXT,   ImmutableList.of(n07,  "",  "",  ""), Type.STRINGS);
        builder.child(n07).setProperty(NEXT,   ImmutableList.of(n06, n05,  "",  ""), Type.STRINGS);
        builder.child(n06).setProperty(NEXT,   ImmutableList.of(n05,  "",  "",  ""), Type.STRINGS);
        builder.child(n05).setProperty(NEXT,   ImmutableList.of(n04, n02, n02,  ""), Type.STRINGS);
        builder.child(n04).setProperty(NEXT,   ImmutableList.of(n03,  "",  "",  ""), Type.STRINGS);
        builder.child(n03).setProperty(NEXT,   ImmutableList.of(n02,  "",  "",  ""), Type.STRINGS);
        builder.child(n02).setProperty(NEXT,   ImmutableList.of(n01,  "",  "",  ""), Type.STRINGS);
        builder.child(n01).setProperty(NEXT,   ImmutableList.of(n00,  "",  "",  ""), Type.STRINGS);
        builder.child(n00).setProperty(NEXT,   ImmutableList.of("" ,  "",  "",  ""), Type.STRINGS);

        NodeState index = builder.getNodeState();
        
        printSkipList(index);
        
        // testing the exception in case of wrong parameters
        String searchFor = "wedontcareaswetesttheexception";
        NodeState node = index.getChildNode(searchFor);
        ChildNodeEntry entry = new OrderedChildNodeEntry(
            searchFor, node);
        ChildNodeEntry[] wl = new ChildNodeEntry[0];
        ChildNodeEntry item = null;
        ChildNodeEntry lane0, lane1, lane2, lane3;
        
        try {
            item = store.seek(index,
                new OrderedContentMirrorStoreStrategy.PredicateEquals(searchFor), wl);
            fail("With a wrong size for the lane it should have raised an exception");
        } catch (IllegalArgumentException e) {
            // so far so good. It was expected
        }
        
        // testing equality
        searchFor = n12;
        lane3 = new OrderedChildNodeEntry(START, index.getChildNode(START));
        lane2 = new OrderedChildNodeEntry(START, index.getChildNode(START));
        lane1 = new OrderedChildNodeEntry(START, index.getChildNode(START));
        lane0 = new OrderedChildNodeEntry(START, index.getChildNode(START));
        entry = new OrderedChildNodeEntry(searchFor,
            index.getChildNode(searchFor));
        wl = new ChildNodeEntry[OrderedIndex.LANES];
        item = store.seek(index,
            new OrderedContentMirrorStoreStrategy.PredicateEquals(searchFor), wl);
        assertNotNull(wl);
        assertEquals(OrderedIndex.LANES, wl.length);
        assertEquals("Wrong lane", lane0, wl[0]);
        assertEquals("Wrong lane", lane1, wl[1]);
        assertEquals("Wrong lane", lane2, wl[2]);
        assertEquals("Wrong lane", lane3, wl[3]);
        assertEquals("Wrong item returned", entry, item);

        searchFor = n08;
        lane3 = new OrderedChildNodeEntry(START, index.getChildNode(START));
        lane2 = new OrderedChildNodeEntry(n09, index.getChildNode(n09));
        lane1 = new OrderedChildNodeEntry(n09, index.getChildNode(n09));
        lane0 = new OrderedChildNodeEntry(n09, index.getChildNode(n09));
        entry = new OrderedChildNodeEntry(searchFor,
            index.getChildNode(searchFor));
        wl = new ChildNodeEntry[OrderedIndex.LANES];
        item = store.seek(index,
            new OrderedContentMirrorStoreStrategy.PredicateEquals(searchFor), wl);
        assertNotNull(wl);
        assertEquals(OrderedIndex.LANES, wl.length);
        assertEquals("Wrong lane", lane0, wl[0]);
        assertEquals("Wrong lane", lane1, wl[1]);
        assertEquals("Wrong lane", lane2, wl[2]);
        assertEquals("Wrong lane", lane3, wl[3]);
        assertEquals("Wrong item returned", entry, item);

        searchFor = n06;
        lane3 = new OrderedChildNodeEntry(START, index.getChildNode(START));
        lane2 = new OrderedChildNodeEntry(n09, index.getChildNode(n09));
        lane1 = new OrderedChildNodeEntry(n07, index.getChildNode(n07));
        lane0 = new OrderedChildNodeEntry(n07, index.getChildNode(n07));
        entry = new OrderedChildNodeEntry(searchFor,
            index.getChildNode(searchFor));
        wl = new ChildNodeEntry[OrderedIndex.LANES];
        item = store.seek(index,
            new OrderedContentMirrorStoreStrategy.PredicateEquals(searchFor), wl);
        assertNotNull(wl);
        assertEquals(OrderedIndex.LANES, wl.length);
        assertEquals("Wrong lane", lane0, wl[0]);
        assertEquals("Wrong lane", lane1, wl[1]);
        assertEquals("Wrong lane", lane2, wl[2]);
        assertEquals("Wrong lane", lane3, wl[3]);
        assertEquals("Wrong item returned", entry, item);
    }
        
    /**
     * convenience method for printing the current index as SkipList
     * 
     * @param index
     */
    private static void printSkipList(NodeState index) {
        final String marker = "->o-";
        final String filler = "----";
        StringBuffer sb = new StringBuffer();
        List<String> elements = new ArrayList<String>();
        
        // printing the elements
        NodeState current = index.getChildNode(START);
        sb.append("STR ");
        
        String next = getNext(current);
        int position = 0;
        while (!Strings.isNullOrEmpty(next)) {
            elements.add(next);
            current = index.getChildNode(next);
            sb.append(String.format("%s ", next));
            next = getNext(current);
        }
        sb.append("NIL");

        for (int lane = 0; lane < OrderedIndex.LANES; lane++) {
            current = index.getChildNode(START);
            sb.append("\n |-");
            next = getNext(current, lane);
            position = 0;
            while (!Strings.isNullOrEmpty(next)) {
                int p = elements.indexOf(next);
                // padding from position to p
                while (position++ < p) {
                    sb.append(filler);
                }
                current = index.getChildNode(next);
                sb.append(marker);
                next = getNext(current, lane);
            }
            //filling the gap towards the end
            while (position++ < elements.size()) {
                sb.append(filler);
            }
            sb.append("->|");
        }

        LOG.debug("\n{}", sb.toString());
    }
    
    @Test
    public void predicateLessThan() { 
        Predicate<ChildNodeEntry> predicate;
        String searchfor;
        ChildNodeEntry entry;
        
        searchfor = "b";
        predicate = new PredicateLessThan(searchfor, true);
        entry = new OrderedChildNodeEntry("a", EmptyNodeState.EMPTY_NODE);
        assertTrue(predicate.apply(entry));

        searchfor = "a";
        predicate = new PredicateLessThan(searchfor, true);
        entry = new OrderedChildNodeEntry("b", EmptyNodeState.EMPTY_NODE);
        assertFalse(predicate.apply(entry));

        searchfor = "a";
        predicate = new PredicateLessThan(searchfor, true);
        entry = null;
        assertFalse(predicate.apply(entry));
    }

    /**
     * tests the pruning with a mult-value index
     */
    @Test
    public void prune() {
        MockOrderedContentMirrorStoreStrategy store = new MockOrderedContentMirrorStoreStrategy();
        NodeBuilder index;
        NodeBuilder node;
        final String path0 = "/content/doc0";
        final String path1 = "/content/doc1";
        final String path2 = "/content/doc2";
        final String n0 = KEYS[0];
        final String n1 = KEYS[1];
        final String n2 = KEYS[2];


        index = EmptyNodeState.EMPTY_NODE.builder();
        store.setLane(0);
        store.update(index, path0, EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, path1, EMPTY_KEY_SET, newHashSet(n1));
        store.update(index, path2, EMPTY_KEY_SET, newHashSet(n2));

        // as we trust the store we skip the check and goes straight to Stage 2.

        // removing n2
        store.update(index, path2, newHashSet(n2), EMPTY_KEY_SET);

        node = index.getChildNode(START);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(n0, "", "", ""), 
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n0);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(n1, "", "", ""), 
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n1);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of("", "", "", ""), 
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n2);
        assertFalse(node.exists());

        // creating
        // STR 000 001 002 NIL
        //  |-->o-->o-->o-->|
        //  |------>o-->o-->|
        //  |---------->o-->|
        //  |---------->o-->|
        index = EmptyNodeState.EMPTY_NODE.builder();
        store.setLane(0);
        store.update(index, path0, EMPTY_KEY_SET, newHashSet(n0));
        store.setLane(1);
        store.update(index, path1, EMPTY_KEY_SET, newHashSet(n1));
        store.setLane(3);
        store.update(index, path2, EMPTY_KEY_SET, newHashSet(n2));

        // and after this update we should have
        // STR 000 001 NIL
        //  |-->o-->o-->|
        //  |------>o-->|
        //  |---------->|
        //  |---------->|
        store.update(index, path2, newHashSet(n2), EMPTY_KEY_SET);

        // checking key nodes
        node = index.getChildNode(START);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(n0, n1, "", ""), 
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n0);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(n1, "", "", ""), 
            node.getProperty(NEXT).getValue(Type.STRINGS));
        node = index.getChildNode(n1);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of("", "", "", ""), 
            node.getProperty(NEXT).getValue(Type.STRINGS));
        assertFalse(index.hasChildNode(n2));
        
        index = EmptyNodeState.EMPTY_NODE.builder();
        store.setLane(0);
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[0]));
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[2]));
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[4]));
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[6]));
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[8]));
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[9]));
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[11]));
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[12]));
        store.setLane(1);
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[1]));
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[5]));
        store.setLane(2);
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[3]));
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[7]));
        store.setLane(3);
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(KEYS[10]));

        store.update(index, "/foo/bar", newHashSet(KEYS[5]), EMPTY_KEY_SET);

        node = index.getChildNode(START);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(KEYS[0], KEYS[1], KEYS[3], KEYS[10]),
            getMultiNext(node.getNodeState()));
        node = index.getChildNode(KEYS[0]);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(KEYS[1], "", "", ""),
            getMultiNext(node.getNodeState()));
        node = index.getChildNode(KEYS[1]);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(KEYS[2], KEYS[3], "", ""),
            getMultiNext(node.getNodeState()));
        node = index.getChildNode(KEYS[2]);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(KEYS[3], "", "", ""),
            getMultiNext(node.getNodeState()));
        node = index.getChildNode(KEYS[3]);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(KEYS[4], KEYS[7], KEYS[7], ""),
            getMultiNext(node.getNodeState()));
        node = index.getChildNode(KEYS[4]);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(KEYS[6], "", "", ""),
            getMultiNext(node.getNodeState()));
        node = index.getChildNode(KEYS[6]);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(KEYS[7], "", "", ""),
            getMultiNext(node.getNodeState()));
        node = index.getChildNode(KEYS[7]);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(KEYS[8], KEYS[10], KEYS[10], ""),
            getMultiNext(node.getNodeState()));
        node = index.getChildNode(KEYS[8]);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(KEYS[9], "", "", ""),
            getMultiNext(node.getNodeState()));
        node = index.getChildNode(KEYS[9]);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(KEYS[10], "", "", ""),
            getMultiNext(node.getNodeState()));
        node = index.getChildNode(KEYS[10]);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(KEYS[11], "", "", ""),
            getMultiNext(node.getNodeState()));
        node = index.getChildNode(KEYS[11]);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of(KEYS[12], "", "", ""),
            getMultiNext(node.getNodeState()));
        node = index.getChildNode(KEYS[12]);
        assertTrue(node.exists());
        assertEquals(ImmutableList.of("", "", "", ""),
            getMultiNext(node.getNodeState()));
        assertFalse(node.getChildNode(KEYS[5]).exists());
    }
    
    /**
     * tests the query aspect of an item that falls int he middle of two lane jumps
     */
    @Test
    public void queryMiddleItem() {
        MockOrderedContentMirrorStoreStrategy ascending = new MockOrderedContentMirrorStoreStrategy(
            OrderDirection.ASC);
        MockOrderedContentMirrorStoreStrategy descending = new MockOrderedContentMirrorStoreStrategy(
            OrderDirection.DESC);
        NodeBuilder index;
        final String propertyName = "property";
        Iterator<String> resultset;
        FilterImpl filter;
        NodeBuilder indexMeta;        
        
        /* generating
         * 
         * STR 000 001 002 003 004 005 NIL
         *  |-->o-->o-->o-->o-->o-->o-->|
         *  |------>o------>o---------->|
         *  |-------------->o---------->|
         *  |-------------------------->|
         */
        index = EmptyNodeState.EMPTY_NODE.builder();
        ascending.setLane(0);
        ascending.update(index, "/path/a", EMPTY_KEY_SET, newHashSet(KEYS[0]));
        ascending.setLane(1);
        ascending.update(index, "/path/b", EMPTY_KEY_SET, newHashSet(KEYS[1]));
        ascending.setLane(0);
        ascending.update(index, "/path/c", EMPTY_KEY_SET, newHashSet(KEYS[2]));
        ascending.setLane(2);
        ascending.update(index, "/path/d", EMPTY_KEY_SET, newHashSet(KEYS[3]));
        ascending.setLane(0);
        ascending.update(index, "/path/e", EMPTY_KEY_SET, newHashSet(KEYS[4]));
        ascending.update(index, "/path/f", EMPTY_KEY_SET, newHashSet(KEYS[5]));

        printSkipList(index.getNodeState());
        
        indexMeta = EmptyNodeState.EMPTY_NODE.builder();
        indexMeta.setChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME, index.getNodeState());

        // querying >= 002
        filter = new FilterImpl();
        filter.restrictProperty(propertyName, Operator.GREATER_OR_EQUAL,
            PropertyValues.newString(KEYS[2]));

        resultset = ascending.query(filter, "indexName", indexMeta.getNodeState(),
            filter.getPropertyRestriction(propertyName)).iterator();
        
        assertEquals("path/c", resultset.next());
        assertEquals("path/d", resultset.next());
        assertEquals("path/e", resultset.next());
        assertEquals("path/f", resultset.next());
        assertFalse("We should have not any results left", resultset.hasNext());
        
        //querying <= 002
        filter = new FilterImpl();
        filter.restrictProperty(propertyName, Operator.LESS_OR_EQUAL,
            PropertyValues.newString(KEYS[2]));
        
        resultset = ascending.query(filter, "indexName", indexMeta.getNodeState(),
            filter.getPropertyRestriction(propertyName)).iterator();

        assertEquals("path/a", resultset.next());
        assertEquals("path/b", resultset.next());
        assertEquals("path/c", resultset.next());
        assertFalse("We should have not any results left", resultset.hasNext());
        
        /*
         * generating
         * 
         * STR 005 004 003 002 001 000 NIL
         *  |-->o-->o-->o-->o-->o-->o-->|
         *  |------>o------>o---------->|
         *  |-------------->o---------->|
         *  |-------------------------->|
         */
        index = EmptyNodeState.EMPTY_NODE.builder();
        descending.setLane(0);
        descending.update(index, "/path/a", EMPTY_KEY_SET, newHashSet(KEYS[5]));
        descending.setLane(1);
        descending.update(index, "/path/b", EMPTY_KEY_SET, newHashSet(KEYS[4]));
        descending.setLane(0);
        descending.update(index, "/path/c", EMPTY_KEY_SET, newHashSet(KEYS[3]));
        descending.setLane(2);
        descending.update(index, "/path/d", EMPTY_KEY_SET, newHashSet(KEYS[2]));
        descending.setLane(0);
        descending.update(index, "/path/e", EMPTY_KEY_SET, newHashSet(KEYS[1]));
        descending.update(index, "/path/f", EMPTY_KEY_SET, newHashSet(KEYS[0]));
        
        printSkipList(index.getNodeState());
        
        indexMeta = EmptyNodeState.EMPTY_NODE.builder();
        indexMeta.setChildNode(IndexConstants.INDEX_CONTENT_NODE_NAME, index.getNodeState());
        
        // querying >= 003
        filter = new FilterImpl();
        filter.restrictProperty(propertyName, Operator.GREATER_OR_EQUAL,
            PropertyValues.newString(KEYS[3]));

        resultset = descending.query(filter, "indexName", indexMeta.getNodeState(),
            filter.getPropertyRestriction(propertyName)).iterator();

        assertEquals("path/a", resultset.next());
        assertEquals("path/b", resultset.next());
        assertEquals("path/c", resultset.next());
        assertFalse("We should have not any results left", resultset.hasNext());

        // querying <= 003
        filter = new FilterImpl();
        filter.restrictProperty(propertyName, Operator.LESS_OR_EQUAL,
            PropertyValues.newString(KEYS[3]));

        resultset = descending.query(filter, "indexName", indexMeta.getNodeState(),
            filter.getPropertyRestriction(propertyName)).iterator();

        assertEquals("path/c", resultset.next());
        assertEquals("path/d", resultset.next());
        assertEquals("path/e", resultset.next());
        assertEquals("path/f", resultset.next());
        assertFalse("We should have not any results left", resultset.hasNext());
    }
}
