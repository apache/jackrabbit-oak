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

import org.apache.jackrabbit.oak.commons.json.JsopBuilder;
import org.apache.jackrabbit.mk.store.RevisionProvider;
import org.apache.jackrabbit.oak.commons.PathUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * JSOP Diff Builder
 */
public class DiffBuilder {

    private final Node before;
    private final Node after;
    private final String path;
    private final int depth;
    private final String pathFilter;
    private final RevisionProvider rp;

    public DiffBuilder(Node before, Node after, String path, int depth,
                       RevisionProvider rp, String pathFilter) {
        this.before = before;
        this.after = after;
        this.path = path;
        this.depth = depth;
        this.rp = rp;
        this.pathFilter = (pathFilter == null || "".equals(pathFilter)) ? "/" : pathFilter;
    }

    public String build() throws Exception {
        final JsopBuilder buff = new JsopBuilder();

        // maps (key: id of target node, value: list of paths to the target)
        // for tracking added/removed nodes; this allows us
        // to detect 'move' operations

        final HashMap<Id, ArrayList<String>> addedNodes = new HashMap<Id, ArrayList<String>>();
        final HashMap<Id, ArrayList<String>> removedNodes = new HashMap<Id, ArrayList<String>>();

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

        TraversingNodeDiffHandler diffHandler = new TraversingNodeDiffHandler(rp) {
            int levels = depth < 0 ? Integer.MAX_VALUE : depth;
            @Override
            public void propAdded(String propName, String value) {
                String p = PathUtils.concat(getCurrentPath(), propName);
                if (p.startsWith(pathFilter)) {
                    buff.tag('^').
                            key(p).
                            encodedValue(value).
                            newline();
                }
            }

            @Override
            public void propChanged(String propName, String oldValue, String newValue) {
                String p = PathUtils.concat(getCurrentPath(), propName);
                if (p.startsWith(pathFilter)) {
                    buff.tag('^').
                            key(p).
                            encodedValue(newValue).
                            newline();
                }
            }

            @Override
            public void propDeleted(String propName, String value) {
                String p = PathUtils.concat(getCurrentPath(), propName);
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
            public void childNodeAdded(ChildNodeEntry added) {
                String p = PathUtils.concat(getCurrentPath(), added.getName());
                if (p.startsWith(pathFilter)) {
                    ArrayList<String> removedPaths = removedNodes.get(added.getId());
                    if (removedPaths != null) {
                        // move detected
                        String removedPath = removedPaths.remove(0);
                        if (removedPaths.isEmpty()) {
                            removedNodes.remove(added.getId());
                        }
                        buff.tag('>').
                                // path/to/deleted/node
                                key(removedPath).
                                // path/to/added/node
                                value(p).
                                newline();
                    } else {
                        ArrayList<String> addedPaths = addedNodes.get(added.getId());
                        if (addedPaths == null) {
                            addedPaths = new ArrayList<String>();
                            addedNodes.put(added.getId(), addedPaths);
                        }
                        addedPaths.add(p);
                    }
                }
            }

            @Override
            public void childNodeDeleted(ChildNodeEntry deleted) {
                String p = PathUtils.concat(getCurrentPath(), deleted.getName());
                if (p.startsWith(pathFilter)) {
                    ArrayList<String> addedPaths = addedNodes.get(deleted.getId());
                    if (addedPaths != null) {
                        // move detected
                        String addedPath = addedPaths.remove(0);
                        if (addedPaths.isEmpty()) {
                            addedNodes.remove(deleted.getId());
                        }
                        buff.tag('>').
                                // path/to/deleted/node
                                key(p).
                                // path/to/added/node
                                value(addedPath).
                                newline();
                    } else {
                        ArrayList<String> removedPaths = removedNodes.get(deleted.getId());
                        if (removedPaths == null) {
                            removedPaths = new ArrayList<String>();
                            removedNodes.put(deleted.getId(), removedPaths);
                        }
                        removedPaths.add(p);
                    }
                }
            }

            @Override
            public void childNodeChanged(ChildNodeEntry changed, Id newId) {
                String p = PathUtils.concat(getCurrentPath(), changed.getName());
                if (PathUtils.isAncestor(p, pathFilter)
                        || p.startsWith(pathFilter)) {
                    --levels;
                    if (levels >= 0) {
                        // recurse
                        super.childNodeChanged(changed, newId);
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
        for (Map.Entry<Id, ArrayList<String>> entry : addedNodes.entrySet()) {
            for (String p : entry.getValue()) {
                buff.tag('+').
                        key(p).object();
                toJson(buff, rp.getNode(entry.getKey()), depth);
                buff.endObject().newline();
            }
        }
        //  ... and removed nodes
        for (Map.Entry<Id, ArrayList<String>> entry : removedNodes.entrySet()) {
            for (String p : entry.getValue()) {
                buff.tag('-');
                buff.value(p);
                buff.newline();
            }
        }
        return buff.toString();
    }

    private void toJson(JsopBuilder builder, Node node, int depth) throws Exception {
        for (Map.Entry<String, String> entry : node.getProperties().entrySet()) {
            builder.key(entry.getKey()).encodedValue(entry.getValue());
        }
        if (depth != 0) {
            for (Iterator<ChildNodeEntry> it = node.getChildNodeEntries(0, -1); it.hasNext(); ) {
                ChildNodeEntry entry = it.next();
                builder.key(entry.getName()).object();
                toJson(builder, rp.getNode(entry.getId()), depth < 0 ? depth : depth - 1);
                builder.endObject();
            }
        }
    }
}
