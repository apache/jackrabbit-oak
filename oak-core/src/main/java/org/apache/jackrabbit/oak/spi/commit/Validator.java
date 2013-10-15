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

import javax.annotation.CheckForNull;

/**
 * Content change validator. An instance of this interface is used to
 * validate changes against a specific {@link NodeState}.
 *
 * @see <a href="http://jackrabbit.apache.org/oak/docs/nodestate.html#Commit_validators"
 *         >Commit validators</a>
 */
public interface Validator extends Editor {

    /**
     * Validate an added property
     * @param after  the added property
     * @throws CommitFailedException  if validation fails.
     */
    void propertyAdded(PropertyState after)
            throws CommitFailedException;

    /**
     * Validate a changed property
     * @param before the original property
     * @param after  the changed property
     * @throws CommitFailedException  if validation fails.
     */
    void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException;

    /**
     * Validate a deleted property
     * @param before the original property
     * @throws CommitFailedException  if validation fails.
     */
    void propertyDeleted(PropertyState before)
            throws CommitFailedException;

    /**
     * Validate an added node
     * @param name the name of the added node
     * @param after  the added node
     * @return a {@code Validator} for {@code after} or {@code null} if validation
     * should not decent into the subtree rooted at {@code after}.
     * @throws CommitFailedException  if validation fails.
     */
    @CheckForNull
    Validator childNodeAdded(String name, NodeState after)
            throws CommitFailedException;

    /**
     * Validate a changed node
     * @param name the name of the changed node
     * @param before the original node
     * @param after  the changed node
     * @return a {@code Validator} for {@code after} or {@code null} if validation
     * should not decent into the subtree rooted at {@code after}.
     * @throws CommitFailedException  if validation fails.
     */
    @CheckForNull
    Validator childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException;

    /**
     * Validate a deleted node
     * @param name The name of the deleted node.
     * @param before the original node
     * @return a {@code Validator} for the removed subtree or
     * {@code null} if validation should not decent into the subtree
     * @throws CommitFailedException  if validation fails.
     */
    @CheckForNull
    Validator childNodeDeleted(String name, NodeState before)
            throws CommitFailedException;

}
