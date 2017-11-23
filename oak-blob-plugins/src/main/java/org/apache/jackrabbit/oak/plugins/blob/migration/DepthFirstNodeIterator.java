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

package org.apache.jackrabbit.oak.plugins.blob.migration;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DepthFirstNodeIterator extends AbstractIterator<ChildNodeEntry> {

    private static final Logger log = LoggerFactory.getLogger(DepthFirstNodeIterator.class);

    private final Deque<Iterator<? extends ChildNodeEntry>> itQueue;

    private final Deque<String> nameQueue;

    private final NodeState root;

    public DepthFirstNodeIterator(NodeState root) {
        this.root = root;
        this.itQueue = new ArrayDeque<>();
        this.nameQueue = new ArrayDeque<>();
        reset();
    }

    private DepthFirstNodeIterator(NodeState root, Deque<Iterator<? extends ChildNodeEntry>> itQueue, Deque<String> nameQueue) {
        this.root = root;
        this.itQueue = itQueue;
        this.nameQueue = nameQueue;
    }

    public void reset() {
        itQueue.clear();
        nameQueue.clear();
        itQueue.add(root.getChildNodeEntries().iterator());
    }

    @Override
    protected ChildNodeEntry computeNext() {
        if (itQueue.isEmpty()) {
            return endOfData();
        }
        if (itQueue.peekLast().hasNext()) {
            ChildNodeEntry next = itQueue.peekLast().next();
            itQueue.add(next.getNodeState().getChildNodeEntries().iterator());
            nameQueue.add(next.getName());
            return next;
        } else {
            itQueue.pollLast();
            if (!nameQueue.isEmpty()) {
                nameQueue.pollLast();
            }
            return computeNext();
        }
    }

    public NodeBuilder getBuilder(NodeBuilder rootBuilder) {
        NodeBuilder builder = rootBuilder;
        for (String name : nameQueue) {
            builder = builder.getChildNode(name);
        }
        return builder;
    }

    public String getPath() {
        StringBuilder path = new StringBuilder("/");
        return Joiner.on('/').appendTo(path, nameQueue).toString();
    }

    public DepthFirstNodeIterator switchRoot(NodeState newRoot) {
        Deque<Iterator<? extends ChildNodeEntry>> newQueue = new ArrayDeque<>();
        NodeState current = newRoot;
        for (String name : nameQueue) {
            boolean found = false;
            Iterator<? extends ChildNodeEntry> it = current.getChildNodeEntries().iterator();
            newQueue.add(it);
            while (it.hasNext()) {
                ChildNodeEntry e = it.next();
                if (name.equals(e.getName())) {
                    current = e.getNodeState();
                    found = true;
                    break;
                }
            }
            if (!found) {
                log.warn("Can't found {} in the new root. Switching to /", getPath());
                return new DepthFirstNodeIterator(newRoot);
            }
        }
        newQueue.add(current.getChildNodeEntries().iterator());
        return new DepthFirstNodeIterator(newRoot, newQueue, nameQueue);
    }
}