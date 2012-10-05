package org.apache.jackrabbit.mongomk.impl.model.tree;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.jackrabbit.mk.model.tree.AbstractChildNode;
import org.apache.jackrabbit.mk.model.tree.ChildNode;
import org.apache.jackrabbit.mk.model.tree.NodeState;
import org.apache.jackrabbit.mk.model.tree.PropertyState;
import org.apache.jackrabbit.mongomk.api.model.Node;

/**
 * FIXME - This is a dummy class to bridge the gap between MongoMK and Oak.
 * Eventually this class should go away and NodeState or StoredNodeAsState
 * should be used instead. Can we default to AbstractNodeState?
 */
public class NodeStateMongo implements NodeState {

    private final Node node;

    public NodeStateMongo(Node node) {
        this.node = node;
    }

    public Node unwrap() {
        return node;
    }

    @Override
    public PropertyState getProperty(String name) {
        for (PropertyState property : getProperties()) {
            if (name.equals(property.getName())) {
                return property;
            }
        }
        return null;
    }

    @Override
    public long getPropertyCount() {
        long count = 0;
        for (PropertyState property : getProperties()) {
            count++;
        }
        return count;
    }

    @Override
    public Iterable<? extends PropertyState> getProperties() {
        return new Iterable<PropertyState>() {
            @Override
            public Iterator<PropertyState> iterator() {
                final Iterator<Map.Entry<String, Object>> iterator =
                        node.getProperties().entrySet().iterator();
                return new Iterator<PropertyState>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }
                    @Override
                    public PropertyState next() {
                        Map.Entry<String, Object> entry = iterator.next();
                        return new SimplePropertyState(
                                entry.getKey(), entry.getValue().toString());
                    }
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    @Override
    public NodeState getChildNode(String name) {
        // FIXME - Not right.
        Set<Node> children = node.getDescendants(false);
        for (Iterator<Node> iterator = children.iterator(); iterator.hasNext();) {
            Node node = iterator.next();
            if (node.getName().equals(name)) {
                return new NodeStateMongo(node);
            }
        }
        return null;
        //Node childNode = node.getChildNode(name);
        //return new NodeStateMongo(childNode);
    }

    @Override
    public long getChildNodeCount() {
        return node.getChildNodeCount();
    }

    @Override
    public Iterable<? extends ChildNode> getChildNodeEntries(final long offset,
            final int count) {
        if (count < -1) {
            throw new IllegalArgumentException("Illegal count: " + count);
        }

        if (offset > Integer.MAX_VALUE) {
            return Collections.emptyList();
        }

        return new Iterable<ChildNode>() {
            @Override
            public Iterator<ChildNode> iterator() {
                final Iterator<Node> iterator =
                        node.getChildNodeEntries((int) offset, count);
                return new Iterator<ChildNode>() {
                    @Override
                    public boolean hasNext() {
                        return iterator.hasNext();
                    }
                    @Override
                    public ChildNode next() {
                        return getChildNodeEntry(iterator.next());
                    }
                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    private ChildNode getChildNodeEntry(final Node entry) {

        return new AbstractChildNode() {
            @Override
            public String getName() {
                return entry.getName();
            }
            @Override
            public NodeState getNode() {
                try {
                    //StoredNode child = provider.getNode(entry.getId());
                    //return new StoredNodeAsState(child, provider);
                    return new NodeStateMongo(entry);
                } catch (Exception e) {
                    throw new RuntimeException("Unexpected error", e);
                }
            }
        };
    }
}