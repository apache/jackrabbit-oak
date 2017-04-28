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
package org.apache.jackrabbit.oak.upgrade;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import org.apache.jackrabbit.commons.iterator.AbstractLazyIterator;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

public class DescendantsIterator extends AbstractLazyIterator<NodeState> {

    private final Deque<Iterator<? extends ChildNodeEntry>> stack = new ArrayDeque<Iterator<? extends ChildNodeEntry>>();

    private final int maxLevel;

    public DescendantsIterator(NodeState root, int maxLevel) {
        this.maxLevel = maxLevel;
        stack.push(root.getChildNodeEntries().iterator());
    }

    @Override
    protected NodeState getNext() {
        if (!fillStack()) {
            return null;
        }
        return stack.peekFirst().next().getNodeState();
    }

    private boolean fillStack() {
        while (stack.size() < maxLevel || !stack.peekFirst().hasNext()) {
            Iterator<? extends ChildNodeEntry> topIterator = stack.peekFirst();
            if (topIterator.hasNext()) {
                final NodeState nextNode = topIterator.next().getNodeState();
                stack.push(nextNode.getChildNodeEntries().iterator());
            } else {
                stack.pop();
                if (stack.isEmpty()) {
                    return false;
                }
            }
        }
        return true;
    }

}
