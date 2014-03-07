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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Set;

import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.plugins.index.property.OrderedIndex.OrderDirection;
import org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;

/**
 *
 */
public class OrderedContentMirrorStorageStrategyTest {
    /**
     * ascending ordered set of keys. Useful for testing
     */
    private static final String[] KEYS = new String[] { "donald", "goofy", "mickey", "minnie" };
    private static final Set<String> EMPTY_KEY_SET = newHashSet();

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
        final String PATH = "/foo/bar";
        final String[] PATH_NODES = Iterables.toArray(PathUtils.elements(PATH), String.class);
        final String N0 = KEYS[0];

        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder node = null;

        store.update(index, PATH, EMPTY_KEY_SET, newHashSet(N0));

        assertFalse(":index should be left alone with not changes", index.hasProperty(NEXT));
        node = index.getChildNode(START);
        assertTrue(":index should have the :start node", node.exists());
        assertEquals(":start should point to n0", N0, node.getString(NEXT));

        node = index.getChildNode(N0);
        assertTrue("n0 should exists in the index", node.exists());
        assertEquals("n0 should point nowhere as it's the last (and only) element", "", node.getString(NEXT));

        // checking content structure below n0
        node = node.getChildNode(PATH_NODES[0]);
        assertTrue("n0 should contain 'foo'", node.exists());
        node = node.getChildNode(PATH_NODES[1]);
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
        final String PATH = "/foo/bar";
        final String N0 = KEYS[0];
        final String N1 = KEYS[1];

        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder node = null;

        store.update(index, PATH, EMPTY_KEY_SET, newHashSet(N0)); // first node
                                                                  // arrives

        node = index.getChildNode(START);
        assertTrue(":index should have :start", node.exists());
        assertEquals(":start should point to n0", N0, node.getString(NEXT));

        node = index.getChildNode(N0);
        assertTrue(":index should have n0", node.exists());
        assertEquals("n0 should point nowhere at this stage", "", node.getString(NEXT));

        store.update(index, PATH, EMPTY_KEY_SET, newHashSet(N1)); // second node
                                                                  // arrives

        node = index.getChildNode(START);
        assertTrue(":index should still have :start", node.exists());
        assertEquals(":start should still point to n0", N0, node.getString(NEXT));

        node = index.getChildNode(N0);
        assertTrue("n0 should still exists", node.exists());
        assertEquals("n0 should point to n1", N1, node.getString(NEXT));

        node = index.getChildNode(N1);
        assertTrue("n1 should exists", node.exists());
        assertEquals("n1 should point nowhere", "", node.getString(NEXT));
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
        final String N0 = KEYS[1];
        final String N1 = KEYS[0];
        final NodeState NODE_0 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, "").getNodeState();
        final NodeState NODE_1 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, N0).getNodeState();
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();

        // setting-up the index structure
        index.child(START).setProperty(NEXT, N1);
        index.setChildNode(N0, NODE_0);
        index.setChildNode(N1, NODE_1);

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
        assertEquals("The first element should be n1", N1, entry.getName());
        assertEquals("Wrong entry returned", NODE_1, entry.getNodeState());
        assertTrue("We should have 1 elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The second element should be n0", N0, entry.getName());
        assertEquals("Wrong entry returned", NODE_0, entry.getNodeState());
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
        final String N0 = KEYS[1];
        final String N1 = KEYS[0];
        final NodeState NODE_0 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, "").getNodeState();
        final NodeState NODE_1 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, N0).getNodeState();
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();

        // setting-up the index structure
        index.child(START).setProperty(NEXT, N1);
        index.setChildNode(N0, NODE_0);
        index.setChildNode(N1, NODE_1);

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
        assertEquals("The first element should be n1", N1, entry.getName());
        assertEquals("Wrong entry returned", NODE_1, entry.getNodeState());
        assertTrue("We should have 1 elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The second element should be n0", N0, entry.getName());
        assertEquals("Wrong entry returned", NODE_0, entry.getNodeState());
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
        final String N0 = KEYS[1];
        final String N1 = KEYS[0];
        final NodeState NODE_START = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, N1).getNodeState();
        final NodeState NODE_0 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, "").getNodeState();
        final NodeState NODE_1 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, N0).getNodeState();
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();

        // setting-up the index structure
        index.setChildNode(START, NODE_START);
        index.setChildNode(N0, NODE_0);
        index.setChildNode(N1, NODE_1);

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
        assertEquals("Wrong entry returned", NODE_START, entry.getNodeState());
        assertTrue("We should still have elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The second element should be n1", N1, entry.getName());
        assertEquals("Wrong entry returned", NODE_1, entry.getNodeState());
        assertTrue("We should still have elements left to loop through", it.hasNext());
        entry = it.next();
        assertEquals("The third element should be n0", N0, entry.getName());
        assertEquals("Wrong entry returned", NODE_0, entry.getNodeState());
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
        NodeState NODE_START = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, "").getNodeState();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();

        // setting-up the index
        index.setChildNode(START, NODE_START);

        Iterable<? extends ChildNodeEntry> children = store.getChildNodeEntries(index.getNodeState(), true);
        assertEquals("Wrong size of Iterable", 1, Iterators.size(children.iterator()));

        Iterator<? extends ChildNodeEntry> it = store.getChildNodeEntries(index.getNodeState(), true).iterator();
        assertTrue("We should have at least 1 element", it.hasNext());
        ChildNodeEntry entry = it.next();
        assertEquals(":start is expected", START, entry.getName());
        assertEquals("wrong node returned", NODE_START, entry.getNodeState());
        assertFalse("We should be at the end of the list", it.hasNext());
    }

    /**
     * test the case where we want an iterator for the children of a brand new
     * index. In this case :start doesn't exists but if we ask for it we should
     * return it.
     */
    @Test
    public void childNodeEntriesNewIndexWithStart() {
        NodeState NODE_START = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, "").getNodeState();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();

        Iterator<? extends ChildNodeEntry> children = store.getChildNodeEntries(index.getNodeState(), true).iterator();
        assertEquals("Wrong number of children", 1, Iterators.size(children));

        children = store.getChildNodeEntries(index.getNodeState(), true).iterator();
        assertTrue("at least one item expected", children.hasNext());
        ChildNodeEntry child = children.next();
        assertEquals(START, child.getName());
        assertEquals(NODE_START, child.getNodeState());
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
        assertEquals(":start is expected to point to the 1st node", key1st, ns.getString(NEXT));
        ns = index.getChildNode(key1st).getNodeState();
        assertTrue("At Stage 1 the first node is expected to point nowhere as it's the last",
                        Strings.isNullOrEmpty(ns.getString(NEXT)));

        // Stage 2
        store.update(index, "/foo/bar", EMPTY_KEY_SET, newHashSet(key2nd));
        ns = index.getChildNode(START).getNodeState();
        assertEquals(":start is expected to point to the 2nd node", key2nd, ns.getString(NEXT));
        ns = index.getChildNode(key1st).getNodeState();
        assertTrue("At stage 2 the first element should point nowhere as it's the last",
                        Strings.isNullOrEmpty(ns.getString(NEXT)));
        ns = index.getChildNode(key2nd).getNodeState();
        assertEquals("At Stage 2 the second element should point to the first one", key1st, ns.getString(NEXT));
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
        assertEquals(":start should point to the first node", n0, index.getChildNode(START).getString(NEXT));
        assertTrue("the first node should point nowhere", Strings.isNullOrEmpty(index.getChildNode(n0).getString(NEXT)));

        // Stage 2
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        assertEquals(":start should point to n1", n1, index.getChildNode(START).getString(NEXT));
        assertEquals("'n1' should point to 'n0'", n0, index.getChildNode(n1).getString(NEXT));
        assertTrue("n0 should still be point nowhere", Strings.isNullOrEmpty(index.getChildNode(n0).getString(NEXT)));

        // Stage 3
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        assertEquals(":start should point to n1", n1, index.getChildNode(START).getString(NEXT));
        assertEquals("n1 should be pointing to n2", n2, index.getChildNode(n1).getString(NEXT));
        assertEquals("n2 should be pointing to n0", n0, index.getChildNode(n2).getString(NEXT));
        assertTrue("n0 should still be the last item of the list",
                        Strings.isNullOrEmpty(index.getChildNode(n0).getString(NEXT)));

        // Stage 4
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));
        assertEquals(":start should point to n1", n1, index.getChildNode(START).getString(NEXT));
        assertEquals("n1 should be pointing to n2", n2, index.getChildNode(n1).getString(NEXT));
        assertEquals("n2 should be pointing to n0", n0, index.getChildNode(n2).getString(NEXT));
        assertEquals("n0 should be pointing to n3", n3, index.getChildNode(n0).getString(NEXT));
        assertTrue("n3 should be the last element", Strings.isNullOrEmpty(index.getChildNode(n3).getString(NEXT)));
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
        final String N0 = KEYS[0];
        final String N1 = KEYS[1];
        final String PATH = "/content/foobar";
        final String[] NODES = Iterables.toArray(PathUtils.elements(PATH), String.class);
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        NodeBuilder node = null;

        // Stage 1
        store.update(index, PATH, EMPTY_KEY_SET, newHashSet(N0));
        node = index.getChildNode(START);
        assertTrue(":start should exists", node.exists());
        assertEquals(":start should point to n0", N0, node.getString(NEXT));

        node = index.getChildNode(N0);
        assertTrue(":index should have n0", node.exists());
        assertTrue("n0 should point nowhere", Strings.isNullOrEmpty(node.getString(NEXT)));

        node = node.getChildNode(NODES[0]);
        assertTrue("n0 should have /content", node.exists());

        node = node.getChildNode(NODES[1]);
        assertTrue("/content should contain /foobar", node.exists());
        assertTrue("/foobar should have match=true", node.getBoolean("match"));

        // Stage 2
        store.update(index, PATH, newHashSet(N0), newHashSet(N1));
        node = index.getChildNode(START);
        assertEquals(":start should now point to N1", N1, node.getString(NEXT));

        node = index.getChildNode(N1);
        assertTrue("N1 should exists", node.exists());
        assertTrue("N1 should point nowhere", Strings.isNullOrEmpty(node.getString(NEXT)));

        node = node.getChildNode(NODES[0]);
        assertTrue("N1 should have /content", node.exists());

        node = node.getChildNode(NODES[1]);
        assertTrue("/content should contain /foobar", node.exists());
        assertTrue("/foobar should have match=true", node.getBoolean("match"));
    }

    /**
     * <p>
     * find a previous item given a key in an index with 1 element only
     * </p>
     * 
     * <p>
     * <i>it relies on the functionality of the store.update() for creating the
     * index</i>
     * </p>
     * 
     * <code>
     *    :index {
     *       :start : { :next=n0 },
     *       n0 = { :next= }
     *    }
     * </code>
     */
    @Test
    public void findPrevious1ItemIndex() {
        final OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy();
        final String N0 = KEYS[0];
        final NodeState NODE_START = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, N0).getNodeState();
        final NodeState NODE_0 = EmptyNodeState.EMPTY_NODE.builder().setProperty(NEXT, "").getNodeState();
        final NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();

        index.setChildNode(START, NODE_START);
        index.setChildNode(N0, NODE_0);

        NodeState indexState = index.getNodeState();
        ChildNodeEntry previous = store.findPrevious(indexState, NODE_0);
        assertNotNull(previous);
        assertEquals("the :start node is expected", NODE_START, previous.getNodeState());
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
        final String PATH0 = "/content/one";
        final String PATH1 = "/content/two";
        final String N0 = KEYS[0];
        final String N1 = KEYS[1];
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();

        // Stage 1 - initialising the index
        store.update(index, PATH0, EMPTY_KEY_SET, newHashSet(N0));
        store.update(index, PATH1, EMPTY_KEY_SET, newHashSet(N0));

        // ensuring the right structure
        assertTrue(index.hasChildNode(START));
        assertTrue(index.hasChildNode(N0));
        assertFalse(index.hasChildNode(N1));

        NodeBuilder node = index.getChildNode(START);
        assertEquals(":start pointing to wrong node", N0, node.getString(NEXT));

        node = index.getChildNode(N0);
        assertTrue("N0 should go nowhere", Strings.isNullOrEmpty(node.getString(NEXT)));

        // checking the first document
        String[] path = Iterables.toArray(PathUtils.elements(PATH0), String.class);
        node = node.getChildNode(path[0]);
        assertTrue(node.exists());
        node = node.getChildNode(path[1]);
        assertTrue(node.exists());
        assertTrue(node.getBoolean("match"));

        path = Iterables.toArray(PathUtils.elements(PATH0), String.class);
        node = index.getChildNode(N0).getChildNode(path[0]);
        assertTrue(node.exists());
        node = node.getChildNode(path[1]);
        assertTrue(node.exists());
        assertTrue(node.getBoolean("match"));

        // Stage 2
        store.update(index, PATH1, newHashSet(N0), newHashSet(N1));
        assertTrue(index.hasChildNode(START));
        assertTrue(index.hasChildNode(N0));
        assertTrue(index.hasChildNode(N1));

        node = index.getChildNode(START);
        assertEquals(":start pointing to wrong node", N0, node.getString(NEXT));

        node = index.getChildNode(N0);
        assertEquals(N1, node.getString(NEXT));
        path = Iterables.toArray(PathUtils.elements(PATH0), String.class);
        node = node.getChildNode(path[0]);
        assertTrue(node.exists());
        node = node.getChildNode(path[1]);
        assertTrue(node.exists());
        assertTrue(node.getBoolean("match"));
        path = Iterables.toArray(PathUtils.elements(PATH1), String.class);
        node = index.getChildNode(N0).getChildNode(path[0]);// we know both the
                                                            // documents share
                                                            // the same /content
        assertFalse("/content/two should no longer be under N0", node.hasChildNode(path[1]));

        node = index.getChildNode(N1);
        assertTrue("N1 should point nowhere", Strings.isNullOrEmpty(node.getString(NEXT)));
        path = Iterables.toArray(PathUtils.elements(PATH1), String.class);
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
        final String N0 = KEYS[0];
        final String PATH = "/sampledoc";
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();

        // Stage 1 - initialising the index
        store.update(index, PATH, EMPTY_KEY_SET, newHashSet(N0));

        // we assume it works and therefore not checking the status of the index
        // let's go straight to Stage 2

        // Stage 2
        store.update(index, PATH, newHashSet(N0), EMPTY_KEY_SET);
        assertFalse("The node should have been removed", index.hasChildNode(N0));
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
        final String N0 = KEYS[0];
        final String DOC1 = "doc1";
        final String DOC2 = "doc2";
        final String PATH1 = "/" + DOC1;
        final String PATH2 = "/" + DOC2;
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();

        store.update(index, PATH1, EMPTY_KEY_SET, newHashSet(N0));
        store.update(index, PATH2, EMPTY_KEY_SET, newHashSet(N0));

        // we trust the store at this point and skip a double-check. Let's move
        // to Stage 2!

        store.update(index, PATH1, newHashSet(N0), EMPTY_KEY_SET);

        assertTrue(index.hasChildNode(START));
        assertTrue(index.hasChildNode(N0));
        assertEquals(":start should still point to N0", N0, index.getChildNode(START).getString(NEXT));
        assertTrue("n0 should point nowhere", Strings.isNullOrEmpty(index.getChildNode(N0).getString(NEXT)));

        assertFalse(index.getChildNode(N0).hasChildNode(DOC1));
        assertTrue(index.getChildNode(N0).hasChildNode(DOC2));
        assertTrue(index.getChildNode(N0).getChildNode(DOC2).getBoolean("match"));
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
        final String PATH0 = "/content/doc0";
        final String PATH1 = "/content/doc1";
        final String PATH2 = "/content/doc2";
        final String N0 = KEYS[2];
        final String N1 = KEYS[0];
        final String N2 = KEYS[1];

        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy();

        // Stage 1
        store.update(index, PATH0, EMPTY_KEY_SET, newHashSet(N0));
        store.update(index, PATH1, EMPTY_KEY_SET, newHashSet(N1));
        store.update(index, PATH2, EMPTY_KEY_SET, newHashSet(N2));

        // as we trust the store we skip the check and goes straight to Stage 2.

        // Stage 2
        store.update(index, PATH2, newHashSet(N2), EMPTY_KEY_SET);

        // checking key nodes
        assertTrue(index.hasChildNode(START));
        assertTrue(index.hasChildNode(N0));
        assertTrue(index.hasChildNode(N1));
        assertFalse(index.hasChildNode(N2));

        // checking pointers
        assertEquals(N1, index.getChildNode(START).getString(NEXT));
        assertEquals(N0, index.getChildNode(N1).getString(NEXT));
        assertTrue(Strings.isNullOrEmpty(index.getChildNode(N0).getString(NEXT)));

        // checking sub-nodes
        String[] subNodes = Iterables.toArray(PathUtils.elements(PATH0), String.class);
        assertTrue(index.getChildNode(N0).hasChildNode(subNodes[0]));
        assertTrue(index.getChildNode(N0).getChildNode(subNodes[0]).hasChildNode(subNodes[1]));
        assertTrue(index.getChildNode(N0).getChildNode(subNodes[0]).getChildNode(subNodes[1]).getBoolean("match"));

        subNodes = Iterables.toArray(PathUtils.elements(PATH1), String.class);
        assertTrue(index.getChildNode(N1).hasChildNode(subNodes[0]));
        assertTrue(index.getChildNode(N1).getChildNode(subNodes[0]).hasChildNode(subNodes[1]));
        assertTrue(index.getChildNode(N1).getChildNode(subNodes[0]).getChildNode(subNodes[1]).getBoolean("match"));
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
        assertEquals(":start should point to the first node", n0, index.getChildNode(START)
                                                                       .getString(NEXT));
        assertTrue("the first node should point nowhere",
                   Strings.isNullOrEmpty(index.getChildNode(n0).getString(NEXT)));
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
    public void descendingOrderInsert2itemsAlreadyOrdered(){
        IndexStoreStrategy store = new OrderedContentMirrorStoreStrategy(OrderDirection.DESC);
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[1];
        String n1 = KEYS[0];

        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        assertEquals(":start should point to the first node", n0, index.getChildNode(START)
                                                                       .getString(NEXT));
        assertEquals("n0 should point to n1",n1, index.getChildNode(n0).getString(NEXT));
        assertTrue("n1 should point nowhere",
                   Strings.isNullOrEmpty(index.getChildNode(n1).getString(NEXT)));
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
        assertEquals(":start should point to n1", n1, index.getChildNode(START).getString(NEXT));
        assertEquals("n0 should point to n3", n3, index.getChildNode(n0).getString(NEXT));
        assertEquals("n1 should point to n2", n2, index.getChildNode(n1).getString(NEXT));
        assertEquals("n2 should point to n1", n0, index.getChildNode(n2).getString(NEXT));
        assertTrue("n3 should point nowhere",
                   Strings.isNullOrEmpty(index.getChildNode(n3).getString(NEXT)));
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
        assertEquals(":start should point to n0", n0, index.getChildNode(START).getString(NEXT));
        assertTrue("n0 should point nowhere",
                   Strings.isNullOrEmpty(index.getChildNode(n0).getString(NEXT)));

        // Stage 2
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        assertEquals(":start should point to n1", n1, index.getChildNode(START).getString(NEXT));
        assertEquals("n1 should point to n0", n0, index.getChildNode(n1).getString(NEXT));
        assertTrue("n0 should point nowhere",
                   Strings.isNullOrEmpty(index.getChildNode(n0).getString(NEXT)));

        // Stage 3
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n2));
        assertEquals(":start should point to n2", n2, index.getChildNode(START).getString(NEXT));
        assertEquals("n2 should point to n1", n1, index.getChildNode(n2).getString(NEXT));
        assertEquals("n1 should point to n0", n0, index.getChildNode(n1).getString(NEXT));
        assertTrue("n0 should point nowhere",
                   Strings.isNullOrEmpty(index.getChildNode(n0).getString(NEXT)));

        // Stage 4
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n3));
        assertEquals(":start should point to n3", n3, index.getChildNode(START).getString(NEXT));
        assertEquals("n3 should point to n2", n2, index.getChildNode(n3).getString(NEXT));
        assertEquals("n2 should point to n1", n1, index.getChildNode(n2).getString(NEXT));
        assertEquals("n1 should point to n0", n0, index.getChildNode(n1).getString(NEXT));
        assertTrue("n0 should point nowhere",
                   Strings.isNullOrEmpty(index.getChildNode(n0).getString(NEXT)));
    }
    
    /**
     * test finding a previous item in a descending ordered index.
     * 
     * <code>
     *      Stage 1
     *      =======
     *      
     *      :index {
     *          :start : { :next=n0 },
     *          n0 : { :next= }
     *      }
     *      
     *      findPrevious(n0)=:start
     *      
     *      Stage 2
     *      =======
     *      
     *      :index {
     *          :start : { :next=n1 },
     *          n0 : { :next= }
     *          n1 : { :next=n0 }
     *      }
     *      
     *      findPrevious(n0)=n1;
     * </code>
     */
    @Test
    public void descendingOrderFindPrevious(){
        OrderedContentMirrorStoreStrategy store = new OrderedContentMirrorStoreStrategy(OrderDirection.DESC);
        NodeBuilder index = EmptyNodeState.EMPTY_NODE.builder();
        String n0 = KEYS[0];
        String n1 = KEYS[1];
        NodeState indexState;
        NodeState previous;
        NodeState node;
        
        //Stage 1
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n0));
        indexState = index.getNodeState();
        node = indexState.getChildNode(n0);
        previous = indexState.getChildNode(START);
        assertEquals(previous,store.findPrevious(indexState, node).getNodeState());
        
        //Stage 2
        store.update(index, "/a/b", EMPTY_KEY_SET, newHashSet(n1));
        indexState = index.getNodeState();
        node = indexState.getChildNode(n0);
        previous = indexState.getChildNode(n1);
        assertEquals(previous,store.findPrevious(indexState, node).getNodeState());
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
}
