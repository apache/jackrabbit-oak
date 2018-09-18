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

import static org.apache.jackrabbit.oak.api.Type.STRING;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.Validator;

/**
 * A {@code MoveDetector} is a validator that can detect certain move operations
 * and reports these to the wrapped {@link MoveValidator} by calling
 * {@link MoveValidator#move(String, String, NodeState)}. That method is called additional
 * to {@link MoveValidator#childNodeAdded(String, NodeState)} for the destination of the move
 * operation and {@link MoveValidator#childNodeDeleted(String, NodeState)} for the source of
 * the move operation.
 * <p>
 * Detection of move operations relies on the presence of the {@link #SOURCE_PATH} property.
 * New nodes with this property set have been moved from the path indicated by the value of the
 * property to its current location.
 * <p>
 * Limitations:
 * <ul>
 *     <li>Moving a moved node only reports one move from the original source to the final
 *     target.</li>
 *     <li>Moving a transiently added node is not reported as a move operation but as an
 *     add operation on the move target.</li>
 *     <li>Removing a moved node is not reported as a move operation but as a remove operation
 *     from the source of the move.</li>
 *     <li>Moving a child node of a transiently moved node is reported from the original
 *     source node, not from the intermediate one.</li>
 *     <li>Moving a node back and forth to its original location is not reported at all.</li>
 * </ul>
 */
public class MoveDetector implements Validator {
    public static final String SOURCE_PATH = ":source-path";

    private final MoveValidator moveValidator;

    public MoveDetector(MoveValidator moveValidator) {
        this.moveValidator = moveValidator;
    }

    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        moveValidator.enter(before, after);
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        moveValidator.enter(before, after);
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        moveValidator.propertyAdded(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        moveValidator.propertyChanged(before, after);
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        moveValidator.propertyDeleted(before);
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        PropertyState sourceProperty = after.getProperty(SOURCE_PATH);
        if (sourceProperty != null) {
            String sourcePath = sourceProperty.getValue(STRING);
            moveValidator.move(name, sourcePath, after);
        }
        MoveValidator childDiff = moveValidator.childNodeAdded(name, after);
        return childDiff == null
                ? null
                : new MoveDetector(childDiff);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        MoveValidator childDiff = moveValidator.childNodeChanged(name, before, after);
        return childDiff == null
                ? null
                : new MoveDetector(childDiff);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        MoveValidator childDiff = moveValidator.childNodeDeleted(name, before);
        return childDiff == null
                ? null
                : new MoveDetector(childDiff);
    }
}
