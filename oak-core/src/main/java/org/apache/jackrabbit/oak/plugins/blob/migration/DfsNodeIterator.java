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

public class DfsNodeIterator extends AbstractIterator<ChildNodeEntry> {

    private final Deque<Iterator<? extends ChildNodeEntry>> itQueue = new ArrayDeque<Iterator<? extends ChildNodeEntry>>();

    private final Deque<String> nameQueue = new ArrayDeque<String>();

    private final NodeState root;

    public DfsNodeIterator(NodeState root) {
        this.root = root;
        reset();
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
            final ChildNodeEntry next = itQueue.peekLast().next();
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
        final StringBuilder path = new StringBuilder("/");
        return Joiner.on('/').appendTo(path, nameQueue).toString();
    }

}