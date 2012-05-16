/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.spi.state;

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.kernel.CoreValueMapper;
import org.apache.jackrabbit.oak.plugins.memory.MemoryChildNodeEntry;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeState;
import org.apache.jackrabbit.oak.util.Function1;
import org.apache.jackrabbit.oak.util.Iterators;
import org.apache.jackrabbit.oak.util.Predicate;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * TODO: this is very raw POC implementation only. WIP
 *
 * Builder for node states based on decorating base node states.
 * Since each decorator is only concerned with a single change, the
 * chain of all decorators also resembles a list of all the changes
 * done to the ultimate base node state. This allows for reconstruction
 * of the operations (i.e. into a JSON diff). Since the order of the
 * operations is preserved, it should also be possible to generate move
 * operations from delete followed by add operation (not currently
 * implemented).
 * Also note, that JSON diff does not have a operation for setNode.
 * These are currently modelled by remove followed by add. The generated
 * change log may thus be overly long. A subsequent consolidation phase
 * on the change log will however greatly reduce the size of such change
 * logs.
 *
 * Distinguishing feature of this approach:
 *
 *   * All changes are kept local within each node.
 *   * Each node can queried for its change log.
 *   * Moves into/out of a subtree will result in add/remove node operations
 *   * Moves within a subtree result in move operations
 *   * Cannot cope detect copy operations. This is not a problem though
 *     since these do not occur transiently.
 *
 * TODO: improve the way setNode created JSON diff (i.e. by consolidation)
 * TODO: detect remove immediately followed by add and replace with move
 */
public class NodeStateBuilder2  {
    private NodeStateBuilder2() {}

    static NodeState setNode(NodeState parent, String name, NodeState child) {
        return parent.getChildNode(name) == null
            ? null
            : new SetNodeDecorator(parent, name, child);
    }

    static NodeState addNode(NodeState parent, String name, NodeState child) {
        return parent.getChildNode(name) == null
            ? new AddNodeDecorator(parent, name, child)
            : null;
    }

    static NodeState addNode(NodeState parent, String name) {
        return parent.getChildNode(name) == null
            ? new AddNodeDecorator(parent, name, MemoryNodeState.EMPTY_NODE)
            : null;
    }

    static NodeState removeNode(NodeState parent, String name) {
        return parent.getChildNode(name) == null
            ? null
            : new RemoveNodeDecorator(parent, name);
    }

    static NodeState setProperty(NodeState parent, PropertyState property) {
        return parent.getProperty(property.getName()) == null
            ? new AddPropertyDecorator(parent, property)
            : new SetPropertyDecorator(parent, property);
    }

    static NodeState removeProperty(NodeState parent, String name) {
        return parent.getProperty(name) == null
            ? null
            : new RemovePropertyDecorator(parent, name);
    }

    interface Listener {
        void node(NodeState node);
        void addNode(String path);
        void removeNode(String path);
        void setProperty(String path, PropertyState propertyState);
        void setNode(String concat);
    }

    static void toJsop(String path, NodeState node, Listener listener) {
        if (node instanceof NodeDecorator) {
            ((NodeDecorator) node).toJsop(path, listener);
        }
        else {
            listener.node(node);
        }
    }

    abstract static class NodeDecorator extends AbstractNodeState {
        final NodeState decorate;

        protected NodeDecorator(NodeState decorate) {
            this.decorate = decorate;
        }

        @Override
        public PropertyState getProperty(String name) {
            return decorate.getProperty(name);
        }

        @Override
        public long getPropertyCount() {
            return decorate.getPropertyCount();
        }

        @Override
        public NodeState getChildNode(String name) {
            return decorate.getChildNode(name);
        }

        @Override
        public long getChildNodeCount() {
            return decorate.getChildNodeCount();
        }

        @Override
        public Iterable<? extends PropertyState> getProperties() {
            return decorate.getProperties();
        }

        @Override
        public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
            return decorate.getChildNodeEntries();
        }

        public void toJsop(String path, Listener listener) {
            NodeStateBuilder2.toJsop(path, decorate, listener);
        }
    }

    /**
     * {@code NodeState} decorator adding a new node state.
     */
    static class AddNodeDecorator extends NodeDecorator {
        private final String childName;
        private final NodeState node;

        /**
         * Construct a new {@code NodeState} from {@code parent} with {@code node} added
         * as new child with name {@code childName}.
         * @param parent
         * @param childName
         * @param node
         * @return
         */
        public AddNodeDecorator(NodeState parent, String childName, NodeState node) {
            super(parent);
            this.childName = childName;
            this.node = node;
        }

        @Override
        public NodeState getChildNode(String name) {
            return childName.equals(name) ? node : super.getChildNode(name);
        }

        @Override
        public long getChildNodeCount() {
            return 1 + super.getChildNodeCount();
        }

        @Override
        public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
            return new Iterable<ChildNodeEntry>() {
                @Override
                public Iterator<ChildNodeEntry> iterator() {
                    return Iterators.chain(
                        AddNodeDecorator.super.getChildNodeEntries().iterator(),
                        Iterators.singleton(new MemoryChildNodeEntry(childName, node)));
                }
            };
        }

        @Override
        public void toJsop(String path, Listener listener) {
            super.toJsop(path, listener);
            listener.addNode(PathUtils.concat(path, childName));
            NodeStateBuilder2.toJsop(PathUtils.concat(path, childName), node, listener);
        }
    }

    /**
     * {@code NodeState} decorator modifying an existing node state to a new node state.
     */
    static class SetNodeDecorator extends NodeDecorator {
        private final String childName;
        private final NodeState node;

        /**
         * Construct a new {@code NodeState} from {@code parent} with child node state
         * {@code childName} replaced with {@code node}.
         * @param parent
         * @param childName
         * @param node
         * @return
         */
        public SetNodeDecorator(NodeState parent, String childName, NodeState node) {
            super(parent);
            this.childName = childName;
            this.node = node;
        }

        @Override
        public NodeState getChildNode(String name) {
            return childName.equals(name) ? node : super.getChildNode(name);
        }

        @Override
        public long getChildNodeCount() {
            return super.getChildNodeCount();
        }

        @Override
        public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
            return new Iterable<ChildNodeEntry>() {
                @Override
                public Iterator<ChildNodeEntry> iterator() {
                    return Iterators.map(SetNodeDecorator.super.getChildNodeEntries().iterator(),
                        new Function1<ChildNodeEntry, ChildNodeEntry>() {
                            @Override
                            public ChildNodeEntry apply(ChildNodeEntry cne) {
                                return childName.equals(cne.getName())
                                    ? new MemoryChildNodeEntry(childName, node)
                                    : cne;
                        }
                    });
                }
            };
        }

        @Override
        public void toJsop(String path, Listener listener) {
            super.toJsop(path, listener);
            listener.setNode(PathUtils.concat(path, childName));
            NodeStateBuilder2.toJsop(PathUtils.concat(path, childName), node, listener);
        }
    }

    /**
     * {@code NodeState} decorator removing a node state
     */
    static class RemoveNodeDecorator extends NodeDecorator {
        private final String childName;

        /**
         * Construct a new {@code NodeState} from {@code parent} with child node state
         * {@code childName} removed.
         * @param parent
         * @param childName
         * @return
         */
        public RemoveNodeDecorator(NodeState parent, String childName) {
            super(parent);
            this.childName = childName;
        }

        @Override
        public NodeState getChildNode(String name) {
            return childName.equals(name) ? null : super.getChildNode(name);
        }

        @Override
        public long getChildNodeCount() {
            return super.getChildNodeCount() - 1;
        }

        @Override
        public Iterable<? extends ChildNodeEntry> getChildNodeEntries() {
            return new Iterable<ChildNodeEntry>() {
                @Override
                public Iterator<ChildNodeEntry> iterator() {
                    return Iterators.filter(RemoveNodeDecorator.super.getChildNodeEntries().iterator(),
                        new Predicate<ChildNodeEntry>() {
                            @Override
                            public boolean evaluate(ChildNodeEntry cne) {
                                return !childName.equals(cne.getName());
                            }
                        }
                    );
                }
            };
        }

        @Override
        public void toJsop(String path, Listener listener) {
            super.toJsop(path, listener);
            listener.removeNode(PathUtils.concat(path, childName));
        }
    }

    /**
     * {@code NodeState} decorator adding a new property state
     */
    static class AddPropertyDecorator extends NodeDecorator {
        private final PropertyState property;

        /**
         * Construct a new {@code NodeState} from {@code parent} with {@code property}
         * added.
         * @param parent
         * @param property
         * @return
         */
        public AddPropertyDecorator(NodeState parent, PropertyState property) {
            super(parent);
            this.property = property;
        }

        @Override
        public PropertyState getProperty(String name) {
            return property.getName().equals(name)
                ? property
                : super.getProperty(name);
        }

        @Override
        public long getPropertyCount() {
            return super.getPropertyCount() + 1;
        }

        @Override
        public Iterable<? extends PropertyState> getProperties() {
            return new Iterable<PropertyState>() {
                @Override
                public Iterator<PropertyState> iterator() {
                    return Iterators.chain(
                        AddPropertyDecorator.super.getProperties().iterator(),
                        Iterators.singleton(property));
                }
            };
        }

        @Override
        public void toJsop(String path, Listener listener) {
            super.toJsop(path, listener);
            listener.setProperty(path, property);
        }
    }

    /**
     * {@code NodeState} decorator modifying an existing property state.
     */
    static class SetPropertyDecorator extends NodeDecorator {
        private final PropertyState property;

        /**
         * Construct a new {@code NodeState} from {@code parent} with {@code property}
         * replaced.
         * @param parent
         * @param property
         * @return
         */
        public SetPropertyDecorator(NodeState parent, PropertyState property) {
            super(parent);
            this.property = property;
        }

        @Override
        public PropertyState getProperty(String name) {
            return property.getName().equals(name)
                ? property
                : super.getProperty(name);
        }

        @Override
        public long getPropertyCount() {
            return super.getPropertyCount();
        }

        @Override
        public Iterable<? extends PropertyState> getProperties() {
            return new Iterable<PropertyState>() {
                @Override
                public Iterator<PropertyState> iterator() {
                    return Iterators.map(SetPropertyDecorator.super.getProperties().iterator(),
                        new Function1<PropertyState, PropertyState>() {
                            @Override
                            public PropertyState apply(PropertyState state) {
                                return property.getName().equals(state.getName())
                                    ? property
                                    : state;
                            }
                        }
                    );
                }
            };
        }

        @Override
        public void toJsop(String path, Listener listener) {
            super.toJsop(path, listener);
            listener.setProperty(path, property);
        }
    }

    /**
     * {@code NodeState} decorator removing an existing property state.
     */
    static class RemovePropertyDecorator extends NodeDecorator {
        private final String propertyName;

        /**
         * Construct a new {@code NodeState} from {@code parent} with {@code propertyName}
         * removed.
         * @param parent
         * @param propertyName
         * @return
         */
        public RemovePropertyDecorator(NodeState parent, String propertyName) {
            super(parent);
            this.propertyName = propertyName;
        }

        @Override
        public PropertyState getProperty(String name) {
            return propertyName.equals(name)
                ? null
                : super.getProperty(name);
        }

        @Override
        public long getPropertyCount() {
            return super.getPropertyCount() - 1;
        }

        @Override
        public Iterable<? extends PropertyState> getProperties() {
            return new Iterable<PropertyState>() {
                @Override
                public Iterator<PropertyState> iterator() {
                    return Iterators.filter(RemovePropertyDecorator.super.getProperties().iterator(),
                        new Predicate<PropertyState>() {
                            @Override
                            public boolean evaluate(PropertyState prop) {
                                return !propertyName.equals(prop.getName());
                            }
                        }
                    );
                }
            };
        }

        @Override
        public void toJsop(String path, Listener listener) {
            super.toJsop(path, listener);
            listener.setProperty(path, null);
        }
    }

    private static String toJson(PropertyState property) {
        if (property == null) {
            return "null";
        }
        else {
            return property.isArray()
                ? CoreValueMapper.toJsonArray(property.getValues())
                : CoreValueMapper.toJsonValue(property.getValue());
        }
    }

    private static String toJson(NodeState node) {
        StringBuilder json = new StringBuilder();
        buildJson(node, json);
        return json.toString();
    }

    private static void buildJson(NodeState node, StringBuilder json) {
        json.append('{');
        String comma = "";
        for (PropertyState property : node.getProperties()) {
            json.append(comma);
            json.append(property.getName()).append(toJson(property));
            comma = ",";
        }

        comma = "";
        for (ChildNodeEntry child : node.getChildNodeEntries()) {
            json.append(comma);
            json.append(child.getName());
            buildJson(child.getNodeState(), json);
            comma = ",";
        }
        json.append('}');
    }


    public static void main(String[] args) {
        Map<String, NodeState> nodes = new HashMap<String, NodeState>();
        nodes.put("a", MemoryNodeState.EMPTY_NODE);
        nodes.put("b", MemoryNodeState.EMPTY_NODE);
        NodeState base = new MemoryNodeState(Collections.<String, PropertyState>emptyMap(), nodes);

//        NodeState r = addNode(base, "copy", removeNode(base, "a"));

//        NodeState r = addNode(base, "copy", removeNode(base, "a"));
//        r = setNode(r, "copy", addNode(base, "c"));

        // >a:b/a
        NodeState a = base.getChildNode("a");
        NodeState b = base.getChildNode("b");
        NodeState r = removeNode(base, "a");
        r = setNode(r, "b", addNode(b, "a", a));

        // >b:c
//        NodeState b = base.getChildNode("b");
//        NodeState r = removeNode(base, "b");
//        r = addNode(r, "c", b);

        System.out.println(r);

        final StringBuilder sb = new StringBuilder();
        toJsop("/", r, new Listener() {
            boolean include;

            @Override
            public void node(NodeState node) {
                if (include) {  // first call is always base
                    sb.append(toJson(node));
                }
                include = true;
            }

            @Override
            public void addNode(String path) {
                sb.append("+\"").append(path).append("\":");
            }

            @Override
            public void removeNode(String path) {
                sb.append("-\"").append(path).append('"');
            }

            @Override
            public void setProperty(String path, PropertyState propertyState) {
                sb.append("^\"").append(PathUtils.concat(path, propertyState.getName()))
                   .append(':').append(toJson(propertyState));
            }

            @Override
            public void setNode(String path) {
                sb.append("-\"").append(path).append('"');
                sb.append("+\"").append(path).append("\":");
            }
        });

        System.out.println(sb.toString());
    }
}
