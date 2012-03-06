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
package org.apache.jackrabbit.mk.util;

import java.util.Iterator;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.simple.NodeImpl;

/**
 * Traverse the nodes in two repositories / revisions / nodes in order to
 * synchronize them or list the differences.
 * <p>
 * If the target is not set, the tool can be used to list or backup the content,
 * for (data store) garbage collection, or similar.
 */
public class Sync {

    private MicroKernel sourceMk, targetMk;
    private String sourceRev, targetRev;
    private String sourcePath, targetPath = "/";
    private boolean useContentHashOptimization;
    private int childNodesPerBatch = 100;

    private Handler handler;

    /**
     * Set the source (required).
     *
     * @param mk the source
     * @param rev the revision
     * @param path the path
     */
    public void setSource(MicroKernel mk, String rev, String path) {
        sourceMk = mk;
        sourceRev = rev;
        sourcePath = path;
    }

    /**
     * Set the target (optional). If not set, the tool assumes no nodes exist on
     * the target.
     *
     * @param mk the target
     * @param rev the revision
     * @param path the path
     */

    public void setTarget(MicroKernel mk, String rev, String path) {
        targetMk = mk;
        targetRev = rev;
        targetPath = path;
    }

    /**
     * Whether to use the content hash optimization if available.
     *
     * @return true if the optimization should be used
     */
    public boolean getUseContentHashOptimization() {
        return useContentHashOptimization;
    }

    /**
     * Use the content hash optimization if available.
     *
     * @param useContentHashOptimization the new value
     */
    public void setUseContentHashOptimization(boolean useContentHashOptimization) {
        this.useContentHashOptimization = useContentHashOptimization;
    }

    /**
     * Get the number of child nodes to request in one call.
     *
     * @return the number of child nodes to request
     */
    public int getChildNodesPerBatch() {
        return childNodesPerBatch;
    }

    /**
     * Set the number of child nodes to request in one call.
     *
     * @param childNodesPerBatch the number of child nodes to request
     */
    public void setChildNodesPerBatch(int childNodesPerBatch) {
        this.childNodesPerBatch = childNodesPerBatch;
    }

    public void run(Handler handler) {
        this.handler = handler;
        visit("");
    }

    public void visit(String relPath) {
        String source = PathUtils.concat(sourcePath, relPath);
        String target = PathUtils.concat(targetPath, relPath);
        NodeImpl s = null, t = null;
        if (sourceMk.nodeExists(source, sourceRev)) {
            s = NodeImpl.parse(sourceMk.getNodes(source, sourceRev, 0, 0, childNodesPerBatch));
        }
        if (targetMk != null && targetMk.nodeExists(target, targetRev)) {
            t = NodeImpl.parse(targetMk.getNodes(target, targetRev, 0, 0, childNodesPerBatch));
        }
        if (s == null || t == null) {
            if (s == t) {
                // both don't exist - ok
                return;
            } else if (s == null) {
                handler.removeNode(target);
                return;
            } else {
                if (!PathUtils.denotesRoot(target)) {
                    handler.addNode(target);
                }
            }
        }
        // properties
        for (int i = 0; i < s.getPropertyCount(); i++) {
            String name = s.getProperty(i);
            String sourceValue = s.getPropertyValue(i);
            String targetValue = t != null && t.hasProperty(name) ? t.getProperty(name) : null;
            if (!sourceValue.equals(targetValue)) {
                handler.setProperty(target, name, sourceValue);
            }
        }
        if (t != null) {
            for (int i = 0; i < t.getPropertyCount(); i++) {
                String name = t.getProperty(i);
                // if it exists in the source, it's already updated
                if (!s.hasProperty(name)) {
                    handler.setProperty(target, name, null);
                }
            }
        }
        // child nodes
        Iterator<String> it = s.getTotalChildNodeCount() > s.getChildNodeCount() ?
                getAllChildNodeNames(sourceMk, source, sourceRev, childNodesPerBatch) :
                s.getChildNodeNames(Integer.MAX_VALUE);
        while (it.hasNext()) {
            String name = it.next();
            visit(PathUtils.concat(relPath, name));
        }
        if (t != null) {
            it = t.getTotalChildNodeCount() > t.getChildNodeCount() ?
                    getAllChildNodeNames(targetMk, target, targetRev, childNodesPerBatch) :
                    t.getChildNodeNames(Integer.MAX_VALUE);
            while (it.hasNext()) {
                String name = it.next();
                if (s.exists(name)) {
                    // if it exists in the source, it's already updated
                } else if (s.getTotalChildNodeCount() > s.getChildNodeCount() &&
                        sourceMk.nodeExists(PathUtils.concat(source, name), sourceRev)) {
                    // if it exists in the source, it's already updated
                    // (in this case, there are many child nodes)
                } else {
                    visit(PathUtils.concat(relPath, name));
                }
            }
        }
    }

    /**
     * Get a child node name iterator that batches node names. This work
     * efficiently for small and big child node lists.
     *
     * @param mk the implementation
     * @param path the path
     * @param rev the revision
     * @param batchSize the batch size
     * @return a child node name iterator
     */
    public static Iterator<String> getAllChildNodeNames(final MicroKernel mk, final String path, final String rev, final int batchSize) {
        return new Iterator<String>() {

            private long offset;
            private Iterator<String> current;

            {
                nextBatch();
            }

            private void nextBatch() {
                NodeImpl n = NodeImpl.parse(mk.getNodes(path, rev, 0, offset, batchSize));
                current = n.getChildNodeNames(Integer.MAX_VALUE);
                offset += batchSize;
            }

            @Override
            public boolean hasNext() {
                if (!current.hasNext()) {
                    nextBatch();
                }
                return current.hasNext();
            }

            @Override
            public String next() {
                if (!current.hasNext()) {
                    nextBatch();
                }
                return current.next();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * The sync handler.
     */
    public interface Handler {

        /**
         * The given node needs to be added to the target.
         *
         * @param path the path
         */
        void addNode(String path);

        /**
         * The given node needs to be removed from the target.
         *
         * @param path the path
         */
        void removeNode(String target);

        /**
         * The given property needs to be set on the target.
         *
         * @param path the path
         * @param property the property name
         * @param value the new value, or null to remove it
         */
        void setProperty(String target, String property, String value);

    }

}
