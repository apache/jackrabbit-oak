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

package org.apache.jackrabbit.oak.jcr.state;

import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.api.MicroKernelException;
import org.apache.jackrabbit.mk.model.PropertyState;
import org.apache.jackrabbit.oak.jcr.state.ChangeTree.Listener;
import org.apache.jackrabbit.oak.jcr.state.ChangeTree.NodeDelta;
import org.apache.jackrabbit.oak.jcr.util.Path;
import org.apache.jackrabbit.oak.jcr.util.Predicate;

import javax.jcr.RepositoryException;

/**
 * {@code TransientSpace} instances use a {@link org.apache.jackrabbit.oak.jcr.state.ChangeTree} to
 * record transient changes in a JCR hierarchy. Changes can be persisted by calling
 * {@link #save()}. A transient space is bound to a specific revision. Calling
 * {@link #refresh(boolean)} updates the revision to the latest.
 */
public class TransientSpace {
    private final MicroKernel microkernel;
    private final String workspace;
    private final ChangeLog changeLog = new ChangeLog();
    private final Listener changeTreeListener = new Listener() {
        @Override
        public void added(NodeDelta nodeDelta) {
            changeLog.addNode(nodeDelta.getPath());
        }

        @Override
        public void removed(NodeDelta nodeDelta) {
            changeLog.removeNode(nodeDelta.getPath());
        }

        @Override
        public void moved(Path source, NodeDelta nodeDelta) {
            changeLog.moveNode(source, nodeDelta.getPath());
        }

        @Override
        public void setProperty(NodeDelta parent, PropertyState state) {
            changeLog.setProperty(parent.getPath(), state);
        }

        @Override
        public void removeProperty(NodeDelta parent, String name) {
            changeLog.removeProperty(parent.getPath(), name);
        }

    };

    private ChangeTree changeTree;
    private String revision;

    /**
     * Create a new transient space for the given {@code workspace}, {@code microkernel}
     * and {@code revision}.
     * @param workspace
     * @param microkernel
     * @param revision
     */
    public TransientSpace(final String workspace, final MicroKernel microkernel, final String revision) {
        this.microkernel = microkernel;
        this.workspace = workspace;
        this.revision = revision;

        changeTree = createChangeTree(workspace, changeTreeListener);
    }

    /**
     * @param path
     * @return the node delta for the given {@code path}. This is either a persisted node in the
     * revision currently bound to this transient space or a transient node or {@code null}
     * if no such node delta exists.
     */
    public NodeDelta getNodeDelta(Path path) {
        return changeTree.getNode(path);
    }

    /**
     * Atomically persist all transient changes
     * @return  the new revision resulting from saving all transient changes.
     * @throws javax.jcr.RepositoryException
     */
    public String save() throws RepositoryException {
        try {
            revision = microkernel.commit("", changeLog.toJsop(), revision, "");
            changeLog.clear();
            changeTree = createChangeTree(workspace, changeTreeListener);
            return revision;
        }
        catch (MicroKernelException e) {
            throw new RepositoryException(e.getMessage(), e);
        }
    }

    /**
     * Refresh to the latest revision of the persistence store. If {@code keepChanges}
     * is {@code true} transient changes are kept, other wise transient changes are discarded.
     * <em>Note</em>: Keeping transient changes might cause conflicts on subsequent save operations.
     * @param keepChanges
     * @return the new revision
     */
    public String refresh(boolean keepChanges) {
        revision = microkernel.getHeadRevision();
        if (!keepChanges) {
            changeLog.clear();
            changeTree = createChangeTree(workspace, changeTreeListener);
        }

        return revision;
    }

    /**
     * @return {@code true} iff the transient space contains transient changes.
     */
    public boolean isDirty() {
        return changeTree.hasChanges();
    }

    //------------------------------------------< private >---

    private ChangeTree createChangeTree(final String workspace, Listener listener) {
        return new ChangeTree(Path.create(workspace), listener, new Predicate<Path>() {
            @Override
            public boolean evaluate(Path path) {
                return microkernel.nodeExists(path.toMkPath(), revision);
            }
        });
    }

}
