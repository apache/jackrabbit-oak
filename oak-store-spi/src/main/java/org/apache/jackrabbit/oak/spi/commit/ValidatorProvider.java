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

import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import javax.annotation.CheckForNull;

/**
 * Extension point for plugging in different kinds of validation rules
 * for content changes.
 *
 * @see <a href="http://jackrabbit.apache.org/oak/docs/nodestate.html#Commit_validators"
 *         >Commit validators</a>
 */
public abstract class ValidatorProvider implements EditorProvider {

    /**
     * Returns a validator for checking the changes between the given
     * two root states.
     *
     * @param before original root state
     * @param after  modified root state
     * @param info   metadata about this commit
     * @return validator for checking the modifications,
     *         or {@code null} if this validator is not needed for this commit
     */
    @CheckForNull
    protected abstract Validator getRootValidator(
            NodeState before, NodeState after, CommitInfo info);

    //----------------------------------------------------< EditorProvider >--

    @Override @CheckForNull
    public final Editor getRootEditor(
            NodeState before, NodeState after,
            NodeBuilder builder, CommitInfo info) {
        return getRootValidator(before, after, info);
    }

}
