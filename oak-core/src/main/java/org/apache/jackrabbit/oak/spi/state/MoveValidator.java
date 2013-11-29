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

package org.apache.jackrabbit.oak.spi.state;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.Validator;

/**
 * A validator that also receives notifications about moved nodes.
 * @see MoveDetector
 */
public interface MoveValidator extends Validator {

    /**
     * Called when a moved node has been detected.
     *
     * @param sourcePath  path of the node before the move
     * @param name        name of the node after the move
     * @param moved       the node state moved here
     * @throws CommitFailedException  if validation fails.
     * remove
     */
    void move(String name, String sourcePath, NodeState moved) throws CommitFailedException;

    @Override
    @CheckForNull
    MoveValidator childNodeAdded(String name, NodeState after) throws CommitFailedException;

    @Override
    @CheckForNull
    MoveValidator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException;

    @Override
    @CheckForNull
    MoveValidator childNodeDeleted(String name, NodeState before) throws CommitFailedException;
}
