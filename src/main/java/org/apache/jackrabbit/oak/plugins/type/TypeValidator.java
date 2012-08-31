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

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.ConstraintViolationException;
import javax.jcr.nodetype.NodeType;
import javax.jcr.nodetype.NodeTypeManager;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;

class TypeValidator implements Validator {
    private final NodeTypeManager ntm;

    public TypeValidator(NodeTypeManager ntm) {
        this.ntm = ntm;
    }

    //-------------------------------------------------------< NodeValidator >

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        validateType(after);
        // TODO: validate added property
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after) throws CommitFailedException {
        validateType(after);
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        // TODO: validate removed property
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        // TODO: validate added child node
        // TODO: get the type for validating the child contents
        return this;
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) throws CommitFailedException {
        // TODO: validate changed child node
        // TODO: get the type to validating the child contents
        return this;
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) {
        // TODO: validate removed child node
        return null;
    }

    private void validateType(PropertyState after) throws CommitFailedException {
        boolean primaryType = JCR_PRIMARYTYPE.equals(after.getName());
        boolean mixinType = JCR_MIXINTYPES.equals(after.getName());
        if (primaryType || mixinType) {
            try {
                for (CoreValue cv : after.getValues()) {
                    String ntName = cv.getString();
                    NodeType nt = ntm.getNodeType(ntName);
                    if (nt.isAbstract()) {
                        throwConstraintViolationException("Can't create node with abstract type: " + ntName);
                    }
                    if (primaryType && nt.isMixin()) {
                        throwConstraintViolationException("Can't assign mixin for primary type: " + ntName);
                    }
                    if (mixinType && !nt.isMixin()) {
                        throwConstraintViolationException("Can't assign primary type for mixin: " + ntName);
                    }
                }
            }
            catch (RepositoryException e) {
                throw new CommitFailedException(e);
            }
        }
    }

    private static void throwConstraintViolationException(String message) throws CommitFailedException {
        throw new CommitFailedException(new ConstraintViolationException(message));
    }

}
