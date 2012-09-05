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
package org.apache.jackrabbit.oak.plugins.unique;

import java.util.List;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitHook;
import org.apache.jackrabbit.oak.spi.commit.ValidatingHook;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import com.google.common.collect.Lists;

/**
 * Commit hook for validating uniqueness constraints and maintaining
 * indices of unique properties.
 * <p>
 * TODO: verify if enforcing uniqueness constraints is really
 * feasible in oak-core taking potential clustering into account
 * <p>
 * TODO: check if constraint validation needs to take property
 * definition into account e.g. a jcr:uuid property that isn't
 * defined by mix:referenceable may not necessarily be subject
 * to the validation.
 */
public class UniqueIndexHook implements CommitHook {

    @Override
    public NodeState processCommit(
            NodeStore store, NodeState before, NodeState after)
            throws CommitFailedException {
        NodeBuilder builder = store.getBuilder(after);
        NodeBuilder system = builder.getChildBuilder("jcr:system");
        NodeBuilder unique = system.getChildBuilder(":unique");

        final List<UniqueIndexValidator> validators = Lists.newArrayList();
        NodeState state = unique.getNodeState();
        for (ChildNodeEntry entry : state.getChildNodeEntries()) {
            validators.add(new UniqueIndexValidator(
                    entry.getName(), entry.getNodeState()));
        }
        if (validators.isEmpty()) {
            return after; // shortcut
        }

        new ValidatingHook(validators).processCommit(store, before, after);

        for (UniqueIndexValidator validator : validators) {
            validator.apply(unique);
        }

        return builder.getNodeState();
    }

}