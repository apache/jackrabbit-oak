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
package org.apache.jackrabbit.oak.spi.commit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.annotation.CheckForNull;

/**
 * Extension point for content change editors. Used by the {@link EditorHook}
 * class to allow multiple components to process content changes during just
 * a single content diff.
 *
 * @since Oak 0.7
 * @see <a href="http://jackrabbit.apache.org/oak/docs/nodestate.html#Commit_editors"
 *         >Commit editors</a>
 */
public interface EditorProvider {

    /**
     * Returns an editor for processing changes between the given two states.
     * Returns {@code null} if the changes don't require processing.
     * <p>
     * An implementation of this method should generally not compare the
     * given before and after states, as the caller is expected to compare
     * the states and invoke the respective callback methods on the
     * {@link Editor} instance returned by this method. Instead the
     * implementation can use the opportunity for other preparatory work.
     *
     * @param before  original root state
     * @param after   modified root state
     * @param builder node builder based on the after state
     * @param info    metadata associated with this commit
     * @return editor for processing the changes, or {@code null}
     * @throws CommitFailedException if processing failed
     */
    @CheckForNull
    Editor getRootEditor(
        NodeState before, NodeState after, NodeBuilder builder, CommitInfo info)
        throws CommitFailedException;

}
