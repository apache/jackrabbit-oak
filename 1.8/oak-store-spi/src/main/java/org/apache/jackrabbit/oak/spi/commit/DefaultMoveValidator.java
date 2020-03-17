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

package org.apache.jackrabbit.oak.spi.commit;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.state.MoveValidator;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@code MoveValidator} that does nothing by default and doesn't recurse into subtrees.
 * Useful as a sentinel or as a base class for more complex validators.
 *
 * @see DefaultValidator
 */
public class DefaultMoveValidator extends DefaultValidator implements MoveValidator {
    @Override
    public void move(String name, String sourcePath, NodeState moved) throws CommitFailedException {
        // do nothing
    }

    @Override
    public MoveValidator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        // do nothing
        return null;
    }

    @Override
    public MoveValidator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        // do nothing
        return null;
    }

    @Override
    public MoveValidator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        // do nothing
        return null;
    }
}
