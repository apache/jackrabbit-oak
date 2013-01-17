/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.nodetype;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.core.ReadOnlyTree;
import org.apache.jackrabbit.oak.spi.commit.SubtreeValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.commit.ValidatorProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * ValidationProvider implementation that returns a {@code SubtreeValidator}
 * that is looking for changes made to /jcr:system/jcr:nodeTypes and
 * is responsible for making sure that any modifications made to node type
 * definitions are valid.
 */
public class RegistrationValidatorProvider implements ValidatorProvider {

    @Nonnull
    @Override
    public Validator getRootValidator(NodeState before, NodeState after) {
        Validator validator = new RegistrationValidator(
                ReadOnlyNodeTypeManager.getInstance(before),
                ReadOnlyNodeTypeManager.getInstance(after),
                new ReadOnlyTree(before), new ReadOnlyTree(after));
        return new SubtreeValidator(validator, JcrConstants.JCR_SYSTEM, NodeTypeConstants.JCR_NODE_TYPES);
    }
}