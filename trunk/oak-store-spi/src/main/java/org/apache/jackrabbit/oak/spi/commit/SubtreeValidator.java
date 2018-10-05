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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.List;

import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Validator that detects changes to a specified subtree and delegates the
 * validation of such changes to another given validator.
 *
 * @see SubtreeExcludingValidator
 * @since Oak 0.3
 */
public class SubtreeValidator extends DefaultValidator {

    private final Validator validator;

    private final String head;

    private final List<String> tail;

    public SubtreeValidator(Validator validator, String... path) {
        this(validator, Arrays.asList(path));
    }

    private SubtreeValidator(Validator validator, List<String> path) {
        this.validator = checkNotNull(validator);
        checkNotNull(path);
        checkArgument(!path.isEmpty());
        this.head = path.get(0);
        this.tail = path.subList(1, path.size());
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) {
        return descend(name);
    }

    @Override
    public Validator childNodeChanged(
            String name, NodeState before, NodeState after) {
        return descend(name);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) {
        return descend(name);
    }

    private Validator descend(String name) {
        if (!head.equals(name)) {
            return null;
        } else if (tail.isEmpty()) {
            return validator;
        } else {
            return new SubtreeValidator(validator, tail);
        }
    }

}
