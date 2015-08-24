package org.apache.jackrabbit.oak.plugins.blob.migration;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import com.google.common.collect.AbstractIterator;

public class DfsNodeIterator extends AbstractIterator<ChildNodeEntry> {

    private final Deque<Iterator<? extends ChildNodeEntry>> itQueue = new ArrayDeque<Iterator<? extends ChildNodeEntry>>();

    private final Deque<String> nameQueue = new ArrayDeque<String>();

    public DfsNodeIterator(NodeState root) {
        itQueue.add(root.getChildNodeEntries().iterator());
        nameQueue.add("");
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
            nameQueue.pollLast();
            return computeNext();
        }
    }

    public Iterable<String> getPath() {
        return nameQueue;
    }
}