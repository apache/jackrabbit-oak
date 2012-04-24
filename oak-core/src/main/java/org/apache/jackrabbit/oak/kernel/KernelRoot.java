/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.kernel;

import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.json.JsonBuilder;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.kernel.KernelTree.Listener;

import javax.jcr.PropertyType;
import java.util.List;

import static org.apache.jackrabbit.mk.util.PathUtils.elements;
import static org.apache.jackrabbit.mk.util.PathUtils.getName;
import static org.apache.jackrabbit.mk.util.PathUtils.getParentPath;

/**
 * This {@code Root} implementation accumulates all changes into a json diff
 * and applies them to the microkernel on {@link #commit()}
 *
 * TODO: review/rewrite when OAK-45 is resolved
 * When the MicroKernel has support for branching and merging private working copies,
 * this implementation could:
 * - directly write every operation through to the private working copy
 * - batch write operations through to the private working copy when the
 *   transient space gets too big.
 * - spool write operations through to the private working copy on a background thread
 */
public class KernelRoot implements Root {

    private final KernelNodeStore store;
    private final String workspaceName;

    /** Base node state of this tree */
    private NodeState base;

    /** Root state of this tree */
    private KernelTree root;

    /** Log of changes to this tree */
    private ChangeLog changeLog = new ChangeLog();

    public KernelRoot(KernelNodeStore store, String workspaceName) {
        this.store = store;
        this.workspaceName = workspaceName;
        this.base = store.getRoot().getChildNode(workspaceName);
        this.root = new KernelTree(base, changeLog);
    }

    @Override
    public boolean move(String sourcePath, String destPath) {
        KernelTree source = getTransientState(sourcePath);
        if (source == null) {
            return false;
        }

        KernelTree destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        return destParent != null && source.move(destParent, destName);

    }

    @Override
    public boolean copy(String sourcePath, String destPath) {
        KernelTree sourceNode = getTransientState(sourcePath);
        if (sourceNode == null) {
            return false;
        }

        KernelTree destParent = getTransientState(getParentPath(destPath));
        String destName = getName(destPath);
        return destParent != null && sourceNode.copy(destParent, destName);

    }

    @Override
    public Tree getTree(String path) {
        return getTransientState(path);
    }

    @Override
    public void refresh() {
        base = store.getRoot().getChildNode(workspaceName);
    }

    @Override
    public void commit() throws CommitFailedException {
        try {
            store.save(this, base);
            changeLog = new ChangeLog();
            base = store.getRoot().getChildNode(workspaceName);
            root = new KernelTree(base, changeLog);
        } catch (MicroKernelException e) {
            throw new CommitFailedException(e);
        }
    }


    //------------------------------------------------------------< internal >---

    /**
     * JSOP representation of the changes done to this tree
     * @return  changes in JSOP representation
     */
    String getChanges() {
        return changeLog.toJsop();
    }

    //------------------------------------------------------------< private >---

    /**
     * Get a transient node state for the node identified by
     * {@code path}
     * @param path  the path to the node state
     * @return  a {@link KernelTree} instance for the item
     *          at {@code path} or {@code null} if no such item exits.
     */
    private KernelTree getTransientState(String path) {
        KernelTree state = root;
        for (String name : elements(path)) {
            state = state.getChild(name);
            if (state == null) {
                return null;
            }
        }
        return state;
    }

    /**
     * Path of the item {@code name} of the given {@code state}
     *
     * @param state
     * @param name The item name.
     * @return relative path of the item {@code name}
     */
    private static String path(Tree state, String name) {
        String path = state.getPath();
        return path.isEmpty() ? name : path + '/' + name;
    }

    private static String encode(CoreValue value) {
        switch (value.getType()) {
            // TODO: deal with all property types.
            case PropertyType.BOOLEAN: return JsonBuilder.encode(value.getBoolean());
            case PropertyType.LONG:    return JsonBuilder.encode(value.getLong());
            case PropertyType.DOUBLE:  return JsonBuilder.encode(value.getDouble());
            case PropertyType.BINARY:  return null; // TODO implement encoding of binaries
            case PropertyType.STRING:  return JsonBuilder.encode(value.getString());
            default: return JsonBuilder.encode(value.getString());
            //case NULL:    return "null"; // TODO
        }
    }

    private static String encode(Iterable<CoreValue> values) {
        StringBuilder sb = new StringBuilder();
        sb.append('[');
        for (CoreValue cv : values) {
            sb.append(encode(cv));
            sb.append(',');
        }
        if (sb.length() > 1) {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(']');
        return sb.toString();
    }

    /**
     * This {@code Listener} implementation records all changes to
     * a associated tree as JSOP.
     */
    private static class ChangeLog implements Listener {
        private final StringBuilder jsop = new StringBuilder();

        @Override
        public void addChild(KernelTree tree, String name) {
            jsop.append("+\"").append(path(tree, name)).append("\":{}");
        }

        @Override
        public void removeChild(KernelTree tree, String name) {
            jsop.append("-\"").append(path(tree, name)).append('"');
        }

        @Override
        public void setProperty(KernelTree tree, String name, CoreValue value) {
            jsop.append("^\"").append(path(tree, name)).append("\":").append(encode(value));
        }

        @Override
        public void setProperty(KernelTree tree, String name, List<CoreValue> values) {
            jsop.append("^\"").append(path(tree, name)).append("\":").append(encode(values));
        }

        @Override
        public void removeProperty(KernelTree tree, String name) {
            jsop.append("^\"").append(path(tree, name)).append("\":null");
        }

        @Override
        public void move(KernelTree tree, String name, KernelTree moved) {
            jsop.append(">\"").append(path(tree, name)).append("\":\"")
                    .append(moved.getPath()).append('"');
        }

        @Override
        public void copy(KernelTree state, String name, KernelTree copied) {
            jsop.append("*\"").append(path(state, name)).append("\":\"")
                    .append(copied.getPath()).append('"');
        }

        public String toJsop() {
            return jsop.toString();
        }
    }
}
