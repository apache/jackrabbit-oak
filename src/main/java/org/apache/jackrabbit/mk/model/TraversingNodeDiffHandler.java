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

import org.apache.jackrabbit.mk.store.RevisionProvider;
import org.apache.jackrabbit.mk.util.PathUtils;

import java.util.Stack;

/**
 *
 */
public abstract class TraversingNodeDiffHandler implements NodeDiffHandler {

    protected final RevisionProvider store;
    protected Stack paths = new Stack();
    
    public TraversingNodeDiffHandler(RevisionProvider store) {
        this.store = store;
    }

    public void start(Node node1, Node node2) throws Exception {
        start(node1, node2, "/");
    }

    public void start(Node node1, Node node2, String path) throws Exception {
        paths.clear();
        paths.push(path);
        try {
            node1.diff(node2, this);
        } catch (RuntimeException e) {
            Throwable root = e.getCause();
            if (root != null && root instanceof Exception) {
                throw (Exception) root;
            } else {
                throw e;
            }
        }
    }
    
    protected String getCurrentPath() {
        return (String) paths.peek();
    }

    public void propAdded(String propName, String value) {
    }

    public void propChanged(String propName, String oldValue, String newValue) {
    }

    public void propDeleted(String propName, String value) {
    }

    public void childNodeAdded(ChildNodeEntry added) {
    }

    public void childNodeDeleted(ChildNodeEntry deleted) {
    }

    public void childNodeChanged(ChildNodeEntry changed, String newId) {
        paths.push(PathUtils.concat(getCurrentPath(), changed.getName()));
        try {
            store.getNode(changed.getId()).diff(store.getNode(newId), this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        paths.pop();
    }
}
