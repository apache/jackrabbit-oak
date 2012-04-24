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
package org.apache.jackrabbit.oak.plugins.type;

import java.util.Collections;
import java.util.Set;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.kernel.NodeState;
import org.apache.jackrabbit.oak.kernel.Validator;

class TypeValidator implements Validator {

    private final Set<String> types;

    public TypeValidator(Set<String> types) {
        this.types = types;
    }

    private void checkTypeExists(PropertyState after)
            throws CommitFailedException {
        Iterable<CoreValue> coreValues = Collections.emptyList();
        if ("jcr:primaryType".equals(after.getName())) {
            coreValues = Collections.singletonList(after.getValue());
        } else if ("jcr:mixinTypes".equals(after.getName())) {
            coreValues = after.getValues();
        }
        for (CoreValue cv : coreValues) {
            String value = cv.getString();
            if (!types.contains(value)) {
                throw new CommitFailedException("Unknown node type: " + value);
            }
        }
    }

    //-------------------------------------------------------< NodeValidator >

    @Override
    public void propertyAdded(PropertyState after)
            throws CommitFailedException {
        checkTypeExists(after);
        // TODO: validate added property
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        checkTypeExists(after);
        // TODO: validate changed property
    }

    @Override
    public void propertyDeleted(PropertyState before)
            throws CommitFailedException {
        // TODO: validate removed property
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after)
            throws CommitFailedException {
        // TODO: validate added child node
        // TODO: get the type for validating the child contents
        return this;
    }

    @Override
    public Validator childNodeChanged(
            String name, NodeState before, NodeState after)
            throws CommitFailedException {
        // TODO: validate changed child node
        // TODO: get the type to validating the child contents
        return this;
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) {
        // TODO: validate removed child node
        return null;
    }

}
