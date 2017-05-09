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

import java.util.List;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Validator that excludes a subtree from the validation process and delegates
 * validation of other changes to another given validator.
 *
 * @see SubtreeValidator
 * @since Oak 0.9
 */
public class SubtreeExcludingValidator extends DefaultValidator {

    private final Validator validator;

    private final String head;

    private final List<String> tail;

    protected SubtreeExcludingValidator(Validator validator, List<String> path) {
        this.validator = checkNotNull(validator);
        checkNotNull(path);
        checkArgument(!path.isEmpty());
        this.head = path.get(0);
        this.tail = path.subList(1, path.size());
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        validator.propertyAdded(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        validator.propertyChanged(before, after);
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        validator.propertyDeleted(before);
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        validator.childNodeAdded(name, after);
        return descend(name);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after)
            throws CommitFailedException {
        validator.childNodeChanged(name, before, after);
        return descend(name);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        validator.childNodeDeleted(name, before);
        return descend(name);
    }

    private Validator descend(String name) {
        if (!head.equals(name)) {
            return validator;
        } else if (tail.isEmpty()) {
            return null;
        } else {
            return createValidator(validator, tail);
        }
    }

    protected SubtreeExcludingValidator createValidator(Validator validator, List<String> path) {
        return new SubtreeExcludingValidator(validator, path);
    }

}
