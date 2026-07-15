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
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Validator that rejects all changes. Useful as a sentinel or as
 * a tool for testing composite validators.
 *
 * @since Oak 0.3
 */
public class FailingValidator implements Validator {

    private final String type;

    private final int code;

    private final String message;

    public FailingValidator() {
        this("Misc", 0, "All changes are rejected");
    }

    public FailingValidator(String type, int code, String message) {
        this.type = type;
        this.code = code;
        this.message = message;
    }

    @Override
    public void enter(NodeState before, NodeState after) {
    }

    @Override
    public void leave(NodeState before, NodeState after) {
    }

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        throw new CommitFailedException(type, code, message);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        throw new CommitFailedException(type, code, message);
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        throw new CommitFailedException(type, code, message);
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        throw new CommitFailedException(type, code, message);
    }

    @Override
    public Validator childNodeChanged(
            String name, NodeState before, NodeState after) {
        return this; // the subtree might not have changed, so recurse
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        throw new CommitFailedException(type, code, message);
    }

}
