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
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateDiff;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

/**
 * This commit hook implementation validates the changes to be committed
 * against all {@link Validator}s provided by the {@link ValidatorProvider}
 * passed to the class' constructor.
 */
public class ValidatingCommitHook implements CommitHook {
    private final ValidatorProvider validatorProvider;

    /**
     * Create a new commit hook which validates the commit against all
     * {@link Validator}s provided by {@code validatorProvider}.
     * @param validatorProvider  validator provider
     */
    public ValidatingCommitHook(ValidatorProvider validatorProvider) {
        this.validatorProvider = validatorProvider;
    }

    @Override
    public NodeState beforeCommit(NodeStore store, NodeState before, NodeState after)
            throws CommitFailedException {

        Validator rootValidator = validatorProvider.getRootValidator(before, after);
        validate(store, before, after, rootValidator);
        return after;
    }

    @Override
    public void afterCommit(NodeStore store, NodeState before, NodeState after) {
        // nothing to do here
    }

    //------------------------------------------------------------< private >---

    /**
     * Checked exceptions don't compose. So we need to hack around. See
     * <ul>
     * <li>http://markmail.org/message/ak67n5k7mr3vqylm</li>
     * <li>http://markmail.org/message/7l26cofhyr3sk5pr</li>
     * <li>http://markmail.org/message/nw7mg4cmgpeqq4i5</li>
     * <li>http://markmail.org/message/bhocbruikljpuhu6</li>
     * </ul>
     */
    private static class BreakOutException extends RuntimeException {
        public BreakOutException(CommitFailedException cause) {
            super(cause);
        }
    }

    private static void validate(final NodeStore store, NodeState before, NodeState after,
            final Validator validator) throws CommitFailedException {

        try {
            store.compare(before, after, new NodeStateDiff() {
                @Override
                public void propertyAdded(PropertyState after) {
                    try {
                        validator.propertyAdded(after);
                    }
                    catch (CommitFailedException e) {
                        throw new BreakOutException(e);
                    }
                }

                @Override
                public void propertyChanged(PropertyState before, PropertyState after) {
                    try {
                        validator.propertyChanged(before, after);
                    }
                    catch (CommitFailedException e) {
                        throw new BreakOutException(e);
                    }
                }

                @Override
                public void propertyDeleted(PropertyState before) {
                    try {
                        validator.propertyDeleted(before);
                    }
                    catch (CommitFailedException e) {
                        throw new BreakOutException(e);
                    }
                }

                @Override
                public void childNodeAdded(String name, NodeState after) {
                    try {
                        Validator childValidator = validator.childNodeAdded(name, after);
                        if (childValidator != null) {
                            validate(after, validator);
                        }
                    }
                    catch (CommitFailedException e) {
                        throw new BreakOutException(e);
                    }
                }

                @Override
                public void childNodeChanged(String name, NodeState before, NodeState after) {
                    try {
                        Validator childValidator = validator.childNodeChanged(name, before, after);
                        if (childValidator != null) {
                            validate(store, before, after, childValidator);
                        }
                    }
                    catch (CommitFailedException e) {
                        throw new BreakOutException(e);
                    }
                }

                @Override
                public void childNodeDeleted(String name, NodeState before) {
                    try {
                        validator.childNodeDeleted(name, before);
                    }
                    catch (CommitFailedException e) {
                        throw new BreakOutException(e);
                    }
                }
            });
        }
        catch (BreakOutException e) {
            throw new CommitFailedException(e);
        }
    }

    private static void validate(NodeState nodeState, Validator validator)
            throws CommitFailedException {

        for (PropertyState property : nodeState.getProperties()) {
            validator.propertyAdded(property);
        }

        for (ChildNodeEntry child : nodeState.getChildNodeEntries()) {
            Validator childValidator = validator.childNodeAdded(
                    child.getName(), child.getNodeState());
            validate(child.getNodeState(), childValidator);
        }
    }
}
