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
package org.apache.jackrabbit.oak.spi.state;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;

public interface NodeStoreBranch {

    /**
     * Returns the base state of this branch.
     *
     * @return base node state
     */
    @Nonnull
    NodeState getBase();

    /**
     * Returns the latest state of the branch.
     *
     * @return root node state
     */
    @Nonnull
    NodeState getRoot();

    /**
     * Updates the state of the content tree.
     *
     * @param newRoot new root node state
     */
    void setRoot(NodeState newRoot);

    /**
     * Moves a node.
     *
     * @param source source path
     * @param target target path
     * @return  {@code true} iff the move succeeded
     */
    boolean move(String source, String target);

    /**
     * Copies a node.
     *
     * @param source source path
     * @param target target path
     * @return  {@code true} iff the copy succeeded
     */
    boolean copy(String source, String target);

    /**
     * Merges the changes in this branch to the main content tree.
     * @param hook  commit hook to apply
     * @return the node state resulting from the merge.
     *
     * @throws CommitFailedException if the merge failed
     */
    @Nonnull
    NodeState merge(CommitHook hook) throws CommitFailedException;

}

