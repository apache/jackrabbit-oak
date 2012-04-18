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
package org.apache.jackrabbit.mk.model;

import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.mk.util.PathUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * JSOP Diff Builder
 */
public class DiffBuilder {

    private final NodeState before;
    private final NodeState after;
    private final String path;
    private final String filter;
    private final NodeStore store;

    public DiffBuilder(NodeState before, NodeState after, String path,
                       NodeStore store, String filter) {
        this.before = before;
        this.after = after;
        this.path = path;
        this.store = store;
        this.filter = filter;
    }

    public String build() throws Exception {
        // TODO extract and evaluate filter criteria specified in 'filter' parameter

        final JsopBuilder buff = new JsopBuilder();
        // maps (key: id of target node, value: path/to/target)
        // for tracking added/removed nodes; this allows us
        // to detect 'move' operations
        final HashMap<NodeState, String> addedNodes = new HashMap<NodeState, String>();
        final HashMap<NodeState, String> removedNodes = new HashMap<NodeState, String>();

        if (before == null) {
            if (after != null) {
                buff.tag('+').key(path).object();
                toJson(buff, after);
                return buff.endObject().newline().toString();
            } else {
                throw new Exception("path doesn't exist in the specified revisions: " + path);
            }
        } else if (after == null) {
            buff.tag('-');
            buff.value(path);
            return buff.newline().toString();
        }

        TraversingNodeDiffHandler diffHandler = new TraversingNodeDiffHandler(store) {
            @Override
            public void propertyAdded(PropertyState after) {
                buff.tag('+').
                        key(PathUtils.concat(getCurrentPath(), after.getName())).
                        encodedValue(after.getEncodedValue()).
                        newline();
            }

            @Override
            public void propertyChanged(PropertyState before, PropertyState after) {
                buff.tag('^').
                        key(PathUtils.concat(getCurrentPath(), after.getName())).
                        encodedValue(after.getEncodedValue()).
                        newline();
            }

            @Override
            public void propertyDeleted(PropertyState before) {
                // since property and node deletions can't be distinguished
                // using the "- <path>" notation we're representing
                // property deletions as "^ <path>:null"
                buff.tag('^').
                        key(PathUtils.concat(getCurrentPath(), before.getName())).
                        value(null).
                        newline();
            }

            @Override
            public void childNodeAdded(String name, NodeState after) {
                addedNodes.put(after, PathUtils.concat(getCurrentPath(), name));
                buff.tag('+').
                        key(PathUtils.concat(getCurrentPath(), name)).object();
                toJson(buff, after);
                buff.endObject().newline();
            }

            @Override
            public void childNodeDeleted(String name, NodeState before) {
                removedNodes.put(before, PathUtils.concat(getCurrentPath(), name));
                buff.tag('-');
                buff.value(PathUtils.concat(getCurrentPath(), name));
                buff.newline();
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
                @Override
                public void propertyAdded(PropertyState after) {
                    buff.tag('+').
                            key(PathUtils.concat(getCurrentPath(), after.getName())).
                            encodedValue(after.getEncodedValue()).
                            newline();
                }

                @Override
                public void propertyChanged(PropertyState before, PropertyState after) {
                    buff.tag('^').
                            key(PathUtils.concat(getCurrentPath(), after.getName())).
                            encodedValue(after.getEncodedValue()).
                            newline();
                }

                @Override
                public void propertyDeleted(PropertyState before) {
                    // since property and node deletions can't be distinguished
                    // using the "- <path>" notation we're representing
                    // property deletions as "^ <path>:null"
                    buff.tag('^').
                            key(PathUtils.concat(getCurrentPath(), before.getName())).
                            value(null).
                            newline();
                }

                @Override
                public void childNodeAdded(String name, NodeState after) {
                    if (addedNodes.containsKey(after)) {
                        // moved node, will be processed separately
                        return;
                    }
                    buff.tag('+').
                            key(PathUtils.concat(getCurrentPath(), name)).object();
                    toJson(buff, after);
                    buff.endObject().newline();
                }

                @Override
                public void childNodeDeleted(String name, NodeState before) {
                    if (addedNodes.containsKey(before)) {
                        // moved node, will be processed separately
                        return;
                    }
                    buff.tag('-');
                    buff.value(PathUtils.concat(getCurrentPath(), name));
                    buff.newline();
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
        for (ChildNodeEntry entry : node.getChildNodeEntries(0, -1)) {
            builder.key(entry.getName()).object();
            toJson(builder, entry.getNode());
            builder.endObject();
        }
    }
}
