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
package org.apache.jackrabbit.oak.spi.commit;

import java.util.Set;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Base implementation of a validator that enforces uniqueness constraints
 * within a given workspace: this includes unique property values for the
 * properties listed in {@link #getPropertyNames()}.
 */
public abstract class UniquePropertyValidator implements Validator {

    // TODO: verify if enforcing uniqueness constraints is really feasible in oak-core
    // TODO: check if constraint validation needs to take property definition into account
    //       e.g. a jcr:uuid property that isn't defined by mix:referenceable may
    //       not necessarily be subject to the validation.

    /**
     * The property names for which the uniqueness constraint needs to
     * be enforced.
     *
     * @return a set of property names.
     */
    @Nonnull
    protected abstract Set<String> getPropertyNames();


    protected void assertUniqueValue(PropertyState property) throws CommitFailedException {
        // TODO:
    }

    //----------------------------------------------------------< Validator >---
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        if (getPropertyNames().contains(after.getName())) {
            assertUniqueValue(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        if (getPropertyNames().contains(after.getName())) {
            assertUniqueValue(after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        // nothing to do
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        return this;
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        return this;
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        return this;
    }
}