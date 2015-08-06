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
