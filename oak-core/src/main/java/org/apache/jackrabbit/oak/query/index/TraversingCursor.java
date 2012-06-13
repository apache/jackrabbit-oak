/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.apache.jackrabbit.oak.query.index;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.simple.NodeImpl;
import org.apache.jackrabbit.oak.commons.PathUtils;
import org.apache.jackrabbit.oak.spi.Cursor;
import java.util.ArrayList;

/**
 * A cursor that reads all nodes in a given subtree.
 */
public class TraversingCursor implements Cursor {

    private final MicroKernel mk;
    private final String revisionId;
    private final int childBlockSize;

    private ArrayList<NodeCursor> nodes = new ArrayList<NodeCursor>();
    private String currentPath;

    public TraversingCursor(MicroKernel mk, String revisionId, int childBlockSize, String path) {
        this.mk = mk;
        this.revisionId = revisionId;
        this.childBlockSize = childBlockSize;
        currentPath = path;
    }

    private boolean loadChildren(String path, long offset) {
        String s = mk.getNodes(path, revisionId, 0, offset, childBlockSize, null);
        if (s == null) {
            return false;
        }
        NodeCursor c = new NodeCursor();
        c.node = NodeImpl.parse(s);
        c.node.setPath(path);
        c.pos = offset;
        nodes.add(c);
        String child = c.node.getChildNodeName(0);
        return child != null;
    }

    @Override
    public String currentPath() {
        return currentPath;
    }

    @Override
    public boolean next() {
        if (nodes == null) {
            currentPath = null;
            return false;
        }
        if (nodes.isEmpty()) {
            if (!mk.nodeExists(currentPath, revisionId)) {
                nodes = null;
                currentPath = null;
                return false;
            }
            loadChildren(currentPath, 0);
            return true;
        }
        while (!nodes.isEmpty()) {
            // next child node in the deepest level
            NodeCursor c = nodes.get(nodes.size() - 1);
            currentPath = c.node.getPath();
            long pos = c.pos++;
            if (pos >= c.node.getTotalChildNodeCount()) {
                // there are no more child nodes
                nodes.remove(nodes.size() - 1);
            } else {
                if (pos > 0 && pos % childBlockSize == 0) {
                    // need to load a new block
                    nodes.remove(nodes.size() - 1);
                    if (loadChildren(currentPath, pos)) {
                        c = nodes.get(nodes.size() - 1);
                        c.pos++;
                    }
                }
                String childName = c.node.getChildNodeName(pos % childBlockSize);
                currentPath = PathUtils.concat(currentPath, childName);
                loadChildren(currentPath, 0);
                return true;
            }
        }
        nodes = null;
        currentPath = null;
        return false;
    }

    static class NodeCursor {
        NodeImpl node;
        long pos;
    }

}
