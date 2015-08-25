package org.apache.jackrabbit.oak.plugins.blob.migration;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;

import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;

import static org.apache.commons.lang.StringUtils.split;
import static org.apache.commons.lang.StringUtils.defaultString;

public class DfsNodeIterator extends AbstractIterator<ChildNodeEntry> {

    private static final Logger log = LoggerFactory.getLogger(DfsNodeIterator.class);

    private final Deque<Iterator<? extends ChildNodeEntry>> itQueue = new ArrayDeque<Iterator<? extends ChildNodeEntry>>();

    private final Deque<String> nameQueue = new ArrayDeque<String>();

    private final NodeState root;

    public DfsNodeIterator(NodeState root) {
        this(root, "/");
    }

    public DfsNodeIterator(NodeState root, String path) {
        this.root = root;
        reset();
        fastForward(defaultString(path));
    }

    private void fastForward(String path) {
        final String[] split = split(path, '/');
        for (final String name : split) {
            if (name.isEmpty()) {
                continue;
            }
            nameQueue.add(name);

            final Iterator<? extends ChildNodeEntry> it = itQueue.peekLast();
            boolean found = false;
            while (it.hasNext()) {
                final ChildNodeEntry entry = it.next();
                if (name.equals(entry.getName())) {
                    itQueue.add(entry.getNodeState().getChildNodeEntries().iterator());
                    found = true;
                    break;
                }
            }
            if (!found) {
                log.warn("Can't find node {} in path {}", name, path);
                reset();
                break;
            }
        }
    }

    private void reset() {
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