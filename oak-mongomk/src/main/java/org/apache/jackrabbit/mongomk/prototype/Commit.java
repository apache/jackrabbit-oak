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
package org.apache.jackrabbit.mongomk.prototype;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsopStream;
import org.apache.jackrabbit.mk.json.JsopWriter;
import org.apache.jackrabbit.mongomk.prototype.DocumentStore.Collection;
import org.apache.jackrabbit.oak.commons.PathUtils;

/**
 * A higher level object representing a commit.
 */
public class Commit {
    
    private final Revision revision;
    private HashMap<String, UpdateOp> operations = new HashMap<String, UpdateOp>();
    private JsopWriter diff = new JsopStream();
    private HashSet<String> changedParents = new HashSet<String>();
    
    Commit(Revision revision) {
        this.revision = revision;
    }

    UpdateOp getUpdateOperationForNode(String path) {
        UpdateOp op = operations.get(path);
        if (op == null) {
            String id = Node.convertPathToDocumentId(path);
            op = new UpdateOp(id, false);
            operations.put(path, op);
        }
        return op;
    }

    public Revision getRevision() {
        return revision;
    }

    void addNode(Node n) {
        if (operations.containsKey(n.path)) {
            throw new MicroKernelException("Node already added: " + n.path);
        }
        operations.put(n.path, n.asOperation(true));
        diff.tag('+').key(n.path);
        diff.object();
        n.append(diff, false);
        diff.endObject();
        diff.newline();
    }
    
    boolean isEmpty() {
        return operations.isEmpty();
    }

    void apply(DocumentStore store) {
        String commitRoot = null;
        ArrayList<UpdateOp> newNodes = new ArrayList<UpdateOp>();
        ArrayList<UpdateOp> changedNodes = new ArrayList<UpdateOp>();
        for (String p : operations.keySet()) {
            if (commitRoot == null) {
                commitRoot = p;
            } else {
                while (!PathUtils.isAncestor(commitRoot, p)) {
                    commitRoot = PathUtils.getParentPath(commitRoot);
                    if (PathUtils.denotesRoot(commitRoot)) {
                        break;
                    }
                }
            }
        }
        addChangedParent(commitRoot);
        // create a "root of the commit" if there is none
        UpdateOp root = getUpdateOperationForNode(commitRoot);
        for (String p : operations.keySet()) {
            UpdateOp op = operations.get(p);
            if (op == root) {
                // apply at the end
            } else if (op.isNew()) {
                newNodes.add(op);
            } else {
                changedNodes.add(op);
            }
        }
        if (changedNodes.size() == 0) {
            // no updates, so we just add the root like the others
            newNodes.add(root);
            root = null;
        }
        store.create(Collection.NODES, newNodes);
        for (UpdateOp op : changedNodes) {
            store.createOrUpdate(Collection.NODES, op);
        }
        if (root != null) {
            store.createOrUpdate(Collection.NODES, root);
        }
    }

    public void removeNode(String path) {
        diff.tag('-').value(path).newline();
    }

    public JsopWriter getDiff() {
        return diff;
    }

    private void addChangedParent(String path) {
        while (true) {
            changedParents.add(path);
            if (PathUtils.denotesRoot(path)) {
                break;
            }
            path = PathUtils.getParentPath(path);
        }
    }
    
    public Set<String> getChangedParents() {
        return changedParents;
    }

}
