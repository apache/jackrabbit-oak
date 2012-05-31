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

import java.util.ArrayList;
import java.util.List;

/**
 * This {@code Validator} aggregates a list of validators into
 * a single validator.
 */
public class CompositeValidator implements Validator {
    private final List<Validator> validators;

    public CompositeValidator(List<Validator> validators) {
        this.validators = validators;
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        for (Validator validator : validators) {
            validator.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {

        for (Validator validator : validators) {
            validator.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        for (Validator validator : validators) {
            validator.propertyDeleted(before);
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        List<Validator> childValidators = new ArrayList<Validator>(validators.size());
        for (Validator validator : validators) {
            childValidators.add(
                    validator.childNodeAdded(name, after));
        }
        return new CompositeValidator(childValidators);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        List<Validator> childValidators = new ArrayList<Validator>(validators.size());
        for (Validator validator : validators) {
            childValidators.add(
                    validator.childNodeChanged(name, before, after));
        }
        return new CompositeValidator(childValidators);
    }

    @Override
    public void childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        for (Validator validator : validators) {
            validator.childNodeDeleted(name, before);
        }
    }
}
