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

import java.util.ArrayList;
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

        // maps (key: the target node, value: list of paths to the target)
        // for tracking added/removed nodes; this allows us
        // to detect 'move' operations

        // TODO performance problem: this class uses NodeState as a hash key,
        // which is not recommended because the hashCode and equals methods
        // of those classes are slow

        final HashMap<NodeState, ArrayList<String>> addedNodes = new HashMap<NodeState, ArrayList<String>>();
        final HashMap<NodeState, ArrayList<String>> removedNodes = new HashMap<NodeState, ArrayList<String>>();

        if (!PathUtils.isAncestor(path, pathFilter)
                && !path.startsWith(pathFilter)) {
            return "";
        }

        if (before == null) {
            if (after != null) {
                buff.tag('+').key(path).object();
                toJson(buff, after, depth);
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
                    buff.tag('^').
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
                    ArrayList<String> removedPaths = removedNodes.get(after);
                    if (removedPaths != null) {
                        // move detected
                        String removedPath = removedPaths.remove(0);
                        if (removedPaths.isEmpty()) {
                            removedNodes.remove(after);
                        }
                        buff.tag('>').
                                // path/to/deleted/node
                                key(removedPath).
                                // path/to/added/node
                                value(p).
                                newline();
                    } else {
                        ArrayList<String> addedPaths = addedNodes.get(after);
                        if (addedPaths == null) {
                            addedPaths = new ArrayList<String>();
                            addedNodes.put(after, addedPaths);
                        }
                        addedPaths.add(p);
                    }
                }

            }

            @Override
            public void childNodeDeleted(String name, NodeState before) {
                String p = PathUtils.concat(getCurrentPath(), name);
                if (p.startsWith(pathFilter)) {
                    ArrayList<String> addedPaths = addedNodes.get(before);
                    if (addedPaths != null) {
                        // move detected
                        String addedPath = addedPaths.remove(0);
                        if (addedPaths.isEmpty()) {
                            addedNodes.remove(before);
                        }
                        buff.tag('>').
                                // path/to/deleted/node
                                key(p).
                                // path/to/added/node
                                value(addedPath).
                                newline();
                    } else {
                        ArrayList<String> removedPaths = removedNodes.get(before);
                        if (removedPaths == null) {
                            removedPaths = new ArrayList<String>();
                            removedNodes.put(before, removedPaths);
                        }
                        removedPaths.add(p);
                    }
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
                        buff.object().endObject();
                        buff.newline();
                    }
                    ++levels;
                }
            }
        };
        diffHandler.start(before, after, path);

        // finally process remaining added nodes ...
        for (Map.Entry<NodeState, ArrayList<String>> entry : addedNodes.entrySet()) {
            for (String p : entry.getValue()) {
                buff.tag('+').
                        key(p).object();
                toJson(buff, entry.getKey(), depth);
                buff.endObject().newline();
            }
        }
        //  ... and removed nodes
        for (Map.Entry<NodeState, ArrayList<String>> entry : removedNodes.entrySet()) {
            for (String p : entry.getValue()) {
                buff.tag('-');
                buff.value(p);
                buff.newline();
            }
        }
        return buff.toString();
    }

    private void toJson(JsopBuilder builder, NodeState node, int depth) {
        for (PropertyState property : node.getProperties()) {
            builder.key(property.getName()).encodedValue(property.getEncodedValue());
        }
        if (depth != 0) {
            for (ChildNode entry : node.getChildNodeEntries(0, -1)) {
                builder.key(entry.getName()).object();
                toJson(builder, entry.getNode(), depth < 0 ? depth : depth - 1);
                builder.endObject();
            }
        }
    }
}
