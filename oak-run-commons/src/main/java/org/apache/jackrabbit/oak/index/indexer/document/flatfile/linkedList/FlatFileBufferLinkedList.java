/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.linkedList;

import org.apache.jackrabbit.oak.commons.conditions.Validate;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateEntry;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.Objects;

/**
 * Linked list implementation which supports multiple iterators. The iterator's state
 * is backed by an actual node in the list. So, modification in the list show up in
 * iterator (assuming thread safely/volatility) getting handled outside of the class.
 */
public class FlatFileBufferLinkedList implements NodeStateEntryList {

    private final ListNode head = new ListNode();
    private ListNode tail = head;

    private int size = 0;
    private long memUsage = 0;
    private final long memLimit;

    public FlatFileBufferLinkedList() {
        this(Long.MAX_VALUE);
    }

    public FlatFileBufferLinkedList(long memLimit) {
        this.memLimit = memLimit;
    }

    public void add(@NotNull NodeStateEntry item) {
        Validate.checkArgument(item != null, "Can't add null to the list");
        long incomingSize = item.estimatedMemUsage();
        long memUsage = estimatedMemoryUsage();
        Validate.checkState(memUsage + incomingSize <= memLimit,
                "Adding item (%s) estimated with %s bytes would increase mem usage beyond upper limit (%s)." +
                        " Current estimated mem usage is %s bytes", item.getPath(), incomingSize, memLimit, memUsage);
        tail.next = new ListNode(item);
        tail = tail.next;
        size++;
        this.memUsage += incomingSize;
    }

    public NodeStateEntry remove() {
        Validate.checkState(!isEmpty(), "Cannot remove item from empty list");
        NodeStateEntry ret = head.next.data;
        head.next.isValid = false;
        head.next = head.next.next;
        size--;
        memUsage -= ret.estimatedMemUsage();
        if (size == 0) {
            tail = head;
        }
        return ret;
    }

    @Override
    public Iterator<NodeStateEntry> iterator() {
        return NodeIterator.iteratorFor(head);
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public boolean isEmpty() {
        return size == 0;
    }

    @Override
    public long estimatedMemoryUsage() {
        return memUsage;
    }

    @Override
    public void close() {
        // ignore
    }

    /**
     * Represents an item in the list.
     */
    static class ListNode {
        private ListNode next;
        private final NodeStateEntry data;
        private boolean isValid = true;

        private ListNode() {
            this.data = null;
            this.next = null;
        }

        ListNode(@NotNull NodeStateEntry data) {
            Objects.requireNonNull(data);
            this.data = data;
            this.next = null;
        }
    }

    static class NodeIterator implements Iterator<NodeStateEntry> {
        private ListNode current;

        static NodeIterator iteratorFor(@NotNull ListNode node) {
            Objects.requireNonNull(node);
            return new NodeIterator(node);
        }

        NodeIterator(@NotNull ListNode start) {
            Objects.requireNonNull(start);
            this.current = start;
        }

        @Override
        public boolean hasNext() {
            return current.next != null;
        }

        @Override
        public NodeStateEntry next() {
            Validate.checkState(hasNext(), "No next");
            current = current.next;
            Validate.checkState(current.isValid, "Can't call next from a removed node");
            return current.data;
        }
    }

}
