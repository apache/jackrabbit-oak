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

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.state.NodeStateUtils;

/**
 * Validator implementation that allows to exclude hidden nodes and/or properties
 * for the validation process.
 */
public class VisibleValidator implements Validator {

    private final Validator validator;
    private final boolean hideNodes;
    private final boolean hideProperties;

    public VisibleValidator(@Nonnull Validator validator, boolean hideNodes, boolean hideProperties) {
        this.validator = validator;
        this.hideNodes = hideNodes;
        this.hideProperties = hideProperties;
    }

    private Validator getValidator(@Nullable Validator validator) {
        if (validator == null) {
            return null;
        } else if (validator instanceof VisibleValidator) {
            return validator;
        } else {
            return new VisibleValidator(validator, hideNodes, hideProperties);
        }
    }

    private boolean isVisibleNode(String name) {
        if (hideNodes) {
            return !NodeStateUtils.isHidden(name);
        } else {
            return true;
        }
    }

    private boolean isVisibleProperty(String name) {
        if (hideProperties) {
            return !NodeStateUtils.isHidden(name);
        } else {
            return true;
        }
    }

    //----------------------------------------------------------< Validator >---

    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
        validator.enter(before, after);
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
        validator.leave(before, after);
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        if (isVisibleProperty(after.getName())) {
            validator.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        if (isVisibleProperty(after.getName())) {
            validator.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        if (isVisibleProperty(before.getName())) {
            validator.propertyDeleted(before);
        }
    }

    @Override @CheckForNull
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        if (isVisibleNode(name)) {
            return getValidator(validator.childNodeAdded(name, after));
        } else {
            return null;
        }
    }

    @Override @CheckForNull
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        if (isVisibleNode(name)) {
            return getValidator(validator.childNodeChanged(name, before, after));
        } else {
            return null;
        }
    }

    @Override @CheckForNull
    public Validator childNodeDeleted(String name, NodeState before)
            throws CommitFailedException {
        if (isVisibleNode(name)) {
            return getValidator(validator.childNodeDeleted(name, before));
        } else {
            return null;
        }
    }
}