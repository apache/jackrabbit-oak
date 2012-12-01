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
package org.apache.jackrabbit.oak.kernel;

import java.util.Map;
import java.util.Set;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.mk.json.JsopBuilder;
import org.apache.jackrabbit.oak.plugins.memory.MemoryNodeBuilder;
import org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.jackrabbit.oak.plugins.memory.ModifiedNodeState.collapse;

class KernelRootBuilder extends MemoryNodeBuilder {

    /**
     * Number of content updates that need to happen before the updates
     * are automatically committed to a branch in the MicroKernel.
     */
    private static final int UPDATE_LIMIT =
            Integer.getInteger("update.limit", Integer.MAX_VALUE);

    private final MicroKernel kernel;

    private String baseRevision;

    private String branchRevision;

    private int updates = 0;

    public KernelRootBuilder(MicroKernel kernel, KernelNodeState state) {
        super(checkNotNull(state));
        this.kernel = checkNotNull(kernel);
        this.baseRevision = state.getRevision();
        this.branchRevision = null;
    }

    //--------------------------------------------------< MemoryNodeBuilder >---

    @Override
    protected MemoryNodeBuilder createChildBuilder(String name) {
        return new KernelNodeBuilder(this, name, this);
    }

    @Override
    protected void updated() {
        if (updates++ > UPDATE_LIMIT) {
            CopyAndMoveAwareJsopDiff diff = new CopyAndMoveAwareJsopDiff();
            compareAgainstBaseState(diff);
            diff.processMovesAndCopies();

            if (branchRevision == null) {
                branchRevision = kernel.branch(baseRevision);
            }
            branchRevision = kernel.commit(
                    "/", diff.toString(), branchRevision, null);

            updates = 0;
        }
    }

    private class CopyAndMoveAwareJsopDiff extends JsopDiff {
        // FIXME this does currently not work. See OAK-463, OAK464
        private final Map<String, NodeState> added;


        private final Set<String> deleted;

        public CopyAndMoveAwareJsopDiff() {
            super(kernel);
            added = Maps.newHashMap();
            deleted = Sets.newHashSet();
        }

        private CopyAndMoveAwareJsopDiff(
                JsopBuilder jsop, String path,
                Map<String, NodeState> added, Set<String> deleted) {
            super(kernel, jsop, path);
            this.added = added;
            this.deleted = deleted;
        }

        public void processMovesAndCopies() {
            for (Map.Entry<String, NodeState> entry : added.entrySet()) {
                NodeState state = entry.getValue();
                String path = entry.getKey();

                KernelNodeState kstate = getKernelBaseState(state);
                String kpath = kstate.getPath();

                if (deleted.remove(kpath)) {
                    jsop.tag('>');
                } else {
                    jsop.tag('*');
                }
                jsop.key(kpath).value(path);

                if (state != kstate) {
                    state.compareAgainstBaseState(
                            kstate, new JsopDiff(kernel, jsop, path));
                }
            }

            for (String path : deleted) {
                jsop.tag('-').value(path);
            }
        }

        //------------------------------------------------------< JsopDiff >--

        @Override
        protected JsopDiff createChildDiff(JsopBuilder jsop, String path) {
            return new CopyAndMoveAwareJsopDiff(jsop, path, added, deleted);
        }

        //-------------------------------------------------< NodeStateDiff >--

        @Override
        public void childNodeAdded(String name, NodeState after) {
            KernelNodeState kstate = getKernelBaseState(after);
            if (kstate != null) {
                added.put(buildPath(name), after);
            } else {
                super.childNodeAdded(name, after);
            }
        }

        
        @Override
        public void childNodeChanged(
                String name, NodeState before, NodeState after) {
            KernelNodeState kstate = getKernelBaseState(after);
            String path = buildPath(name);
            if (kstate != null && !path.equals(kstate.getPath())) {
                deleted.add(path);
                added.put(path, after);
            } else {
                super.childNodeChanged(name, before, after);
            }
        }

        @Override
        public void childNodeDeleted(String name, NodeState before) {
            deleted.add(buildPath(name));
        }

        //-------------------------------------------------------< private >--

        private KernelNodeState getKernelBaseState(NodeState state) {
            if (state instanceof ModifiedNodeState) {
                state = collapse((ModifiedNodeState) state).getBaseState();
            }

            if (state instanceof KernelNodeState) {
                KernelNodeState kstate = (KernelNodeState) state;
                String arev = kstate.getRevision();
                String brev = branchRevision;
                if (brev == null) {
                    brev = baseRevision;
                }
                if (arev.equals(brev)) {
                    return kstate;
                }
            }

            return null;
        }

    }

}
