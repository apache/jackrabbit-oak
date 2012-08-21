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
package org.apache.jackrabbit.mk.model.tree;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.commons.PathUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * JSOP Diff Builder
 */
public class DiffBuilder {

    private final NodeState before;
    private final NodeState after;
    private final String path;
    private final int depth;
    private final String pathFilter;
    private final NodeStore store;

    public DiffBuilder(NodeState before, NodeState after, String path, int depth,
                       NodeStore store, String pathFilter) {
        this.before = before;
        this.after = after;
        this.path = path;
        this.depth = depth;
        this.store = store;
        this.pathFilter = (pathFilter == null || "".equals(pathFilter)) ? "/" : pathFilter;
    }

    public String build() throws Exception {
        final JsopBuilder buff = new JsopBuilder();
        // maps (key: id of target node, value: path/to/target)
        // for tracking added/removed nodes; this allows us
        // to detect 'move' operations
        final HashMap<NodeState, String> addedNodes = new HashMap<NodeState, String>();
        final HashMap<NodeState, String> removedNodes = new HashMap<NodeState, String>();

        if (!PathUtils.isAncestor(path, pathFilter)
                && !path.startsWith(pathFilter)) {
            return "";
        }

        if (before == null) {
            if (after != null) {
                buff.tag('+').key(path).object();
                toJson(buff, after);
                return buff.endObject().newline().toString();
            } else {
                // path doesn't exist in the specified revisions
                return "";
            }
        } else if (after == null) {
            buff.tag('-');
            buff.value(path);
            return buff.newline().toString();
        }

        TraversingNodeDiffHandler diffHandler = new TraversingNodeDiffHandler(store) {
            int levels = depth < 0 ? Integer.MAX_VALUE : depth;
            @Override
            public void propertyAdded(PropertyState after) {
                String p = PathUtils.concat(getCurrentPath(), after.getName());
                if (p.startsWith(pathFilter)) {
                    buff.tag('+').
                            key(p).
                            encodedValue(after.getEncodedValue()).
                            newline();
                }
            }

            @Override
            public void propertyChanged(PropertyState before, PropertyState after) {
                String p = PathUtils.concat(getCurrentPath(), after.getName());
                if (p.startsWith(pathFilter)) {
                    buff.tag('^').
                            key(p).
                            encodedValue(after.getEncodedValue()).
                            newline();
                }
            }

            @Override
            public void propertyDeleted(PropertyState before) {
                String p = PathUtils.concat(getCurrentPath(), before.getName());
                if (p.startsWith(pathFilter)) {
                    // since property and node deletions can't be distinguished
                    // using the "- <path>" notation we're representing
                    // property deletions as "^ <path>:null"
                    buff.tag('^').
                            key(p).
                            value(null).
                            newline();
                }
            }

            @Override
            public void childNodeAdded(String name, NodeState after) {
                String p = PathUtils.concat(getCurrentPath(), name);
                if (p.startsWith(pathFilter)) {
                    addedNodes.put(after, p);
                    buff.tag('+').
                            key(p).object();
                    toJson(buff, after);
                    buff.endObject().newline();
                }
            }

            @Override
            public void childNodeDeleted(String name, NodeState before) {
                String p = PathUtils.concat(getCurrentPath(), name);
                if (p.startsWith(pathFilter)) {
                    removedNodes.put(before, p);
                    buff.tag('-');
                    buff.value(p);
                    buff.newline();
                }
            }

            @Override
            public void childNodeChanged(String name, NodeState before, NodeState after) {
                String p = PathUtils.concat(getCurrentPath(), name);
                if (PathUtils.isAncestor(p, pathFilter)
                        || p.startsWith(pathFilter)) {
                    --levels;
                    if (levels >= 0) {
                        // recurse
                        super.childNodeChanged(name, before, after);
                    } else {
                        buff.tag('^');
                        buff.key(p);
                        buff.newline();
                    }
                    ++levels;
                }
            }
        };
        diffHandler.start(before, after, path);

        // check if this commit includes 'move' operations
        // by building intersection of added and removed nodes
        addedNodes.keySet().retainAll(removedNodes.keySet());
        if (!addedNodes.isEmpty()) {
            // this commit includes 'move' operations
            removedNodes.keySet().retainAll(addedNodes.keySet());
            // addedNodes & removedNodes now only contain information about moved nodes

            // re-build the diff in a 2nd pass, this time representing moves correctly
            buff.resetWriter();

            // TODO refactor code, avoid duplication

            diffHandler = new TraversingNodeDiffHandler(store) {
                int levels = depth < 0 ? Integer.MAX_VALUE : depth;
                @Override
                public void propertyAdded(PropertyState after) {
                    String p = PathUtils.concat(getCurrentPath(), after.getName());
                    if (p.startsWith(pathFilter)) {
                        buff.tag('+').
                                key(p).
                                encodedValue(after.getEncodedValue()).
                                newline();
                    }
                }

                @Override
                public void propertyChanged(PropertyState before, PropertyState after) {
                    String p = PathUtils.concat(getCurrentPath(), after.getName());
                    if (p.startsWith(pathFilter)) {
                        buff.tag('^').
                                key(p).
                                encodedValue(after.getEncodedValue()).
                                newline();
                    }
                }

                @Override
                public void propertyDeleted(PropertyState before) {
                    String p = PathUtils.concat(getCurrentPath(), before.getName());
                    if (p.startsWith(pathFilter)) {
                        // since property and node deletions can't be distinguished
                        // using the "- <path>" notation we're representing
                        // property deletions as "^ <path>:null"
                        buff.tag('^').
                                key(p).
                                value(null).
                                newline();
                    }
                }

                @Override
                public void childNodeAdded(String name, NodeState after) {
                    if (addedNodes.containsKey(after)) {
                        // moved node, will be processed separately
                        return;
                    }
                    String p = PathUtils.concat(getCurrentPath(), name);
                    if (p.startsWith(pathFilter)) {
                        buff.tag('+').
                                key(p).object();
                        toJson(buff, after);
                        buff.endObject().newline();
                    }
                }

                @Override
                public void childNodeDeleted(String name, NodeState before) {
                    if (addedNodes.containsKey(before)) {
                        // moved node, will be processed separately
                        return;
                    }
                    String p = PathUtils.concat(getCurrentPath(), name);
                    if (p.startsWith(pathFilter)) {
                        buff.tag('-');
                        buff.value(p);
                        buff.newline();
                    }
                }

                @Override
                public void childNodeChanged(String name, NodeState before, NodeState after) {
                    String p = PathUtils.concat(getCurrentPath(), name);
                    if (PathUtils.isAncestor(p, pathFilter)
                            || p.startsWith(pathFilter)) {
                        --levels;
                        if (levels >= 0) {
                            // recurse
                            super.childNodeChanged(name, before, after);
                        } else {
                            buff.tag('^');
                            buff.value(p);
                            buff.newline();
                        }
                        ++levels;
                    }
                }
            };
            diffHandler.start(before, after, path);

            // finally process moved nodes
            for (Map.Entry<NodeState, String> entry : addedNodes.entrySet()) {
                buff.tag('>').
                        // path/to/deleted/node
                        key(removedNodes.get(entry.getKey())).
                        // path/to/added/node
                        value(entry.getValue()).
                        newline();
            }
        }
        return buff.toString();
    }

    private void toJson(JsopBuilder builder, NodeState node) {
        for (PropertyState property : node.getProperties()) {
            builder.key(property.getName()).encodedValue(property.getEncodedValue());
        }
        for (ChildNode entry : node.getChildNodeEntries(0, -1)) {
            builder.key(entry.getName()).object();
            toJson(builder, entry.getNode());
            builder.endObject();
        }
    }
}
