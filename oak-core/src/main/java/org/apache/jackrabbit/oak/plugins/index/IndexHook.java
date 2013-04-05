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
package org.apache.jackrabbit.oak.plugins.index;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Represents the content of a QueryIndex as well as a mechanism for keeping
 * this content up to date. <br>
 * An IndexHook listens for changes to the content and updates the index data
 * accordingly.
 */
public interface IndexHook extends Editor {

    /**
     * Return an editor that can be used to recreate this index, or
     * <code>null</code> if reindexing is not required or is taken care of by
     * the impl directly using the provided state as a reference <br>
     * <br>
     * By providing an Editor an impl could help the IndexManager gain some
     * performance on account of doing the reindexing in parallel for all
     * indexers <br>
     * <br>
     * <i>Note:</i> All the existing IndexHook impls require a call to
     * {@link #enter(NodeState, NodeState)} to build initial state before
     * calling {@link #reindex(NodeState)}, this is enforced via the
     * IndexManager.
     * 
     * @param state
     *            state can be used to reindex inside the IndexHook directly,
     *            instead of providing an Editor
     * 
     */
    @CheckForNull
    Editor reindex(NodeState state) throws CommitFailedException;

}
