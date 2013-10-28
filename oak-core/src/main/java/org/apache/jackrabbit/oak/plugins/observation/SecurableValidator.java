/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.plugins.observation;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Base class for {@code Validator} implementations that can be secured.
 * That is, with exception of {@link Validator#childNodeChanged(String, NodeState, NodeState)},
 * the call back methods of the wrapped validator are only called when its receiver has sufficient
 * rights to access the respective items.
 * <p>
 * Implementors must implement the {@link #create(Tree, Tree, Validator)} factory method for
 * creating {@code SecurableValidator} instances. Further implementors should override
 * {@link #canRead(Tree, PropertyState, Tree, PropertyState)} and {@link #canRead(Tree, Tree)}
 * to determine whether the passed states are accessible. Finally implementors should override,
 * {@link #secureBefore(String, NodeState)}, and {@link #secureAfter(String, NodeState)}} wrapping
 * the passed node state into a node state that restricts access to accessible child nodes and
 * properties.
 */
public abstract class SecurableValidator implements Validator {

    /**
     * Base tree against which the validation is performed
     */
    private final Tree beforeTree;

    /**
     * Changed tree against which the validation is performed
     */
    private final Tree afterTree;

    /**
     * Validator receiving call backs for accessible items
     */
    private final Validator validator;

    /**
     * Create a new instance wrapping the passed {@code validator}.
     * @param beforeTree base tree against which the validation is performed
     * @param afterTree  changed tree against which the validation is performed
     * @param validator  the wrapped validator
     */
    protected SecurableValidator(Tree beforeTree, Tree afterTree, Validator validator) {
        this.beforeTree = beforeTree;
        this.afterTree = afterTree;
        this.validator = validator;
    }

    /**
     * Factory method for creating {@code SecurableValidator} instances of the concrete sub type.
     * @return  a new {@code SecurableValidator}
     */
    @CheckForNull
    protected abstract SecurableValidator create(Tree beforeTree, Tree afterTree,
            Validator secureValidator);

    /**
     * Determine whether a property is accessible
     * @param beforeParent parent before the changes
     * @param before  before state of the property
     * @param afterParent parent after the changes
     * @param after   after state of the property
     * @return  {@code true} if accessible, {@code false} otherwise.
     */
    protected boolean canRead(Tree beforeParent, PropertyState before, Tree afterParent,
            PropertyState after) {
        return true;
    }

    /**
     * Determine whether a node is accessible
     * @param before  before state of the node
     * @param after   after state of the node
     * @return  {@code true} if accessible, {@code false} otherwise.
     */
    protected boolean canRead(Tree before, Tree after) {
        return true;
    }

    /**
     * Secure the before state of a child node such that it only provides
     * accessible child nodes and properties.
     * @param name       name of the child node
     * @param nodeState  before state of the child node
     * @return  secured before state
     */
    @Nonnull
    protected NodeState secureBefore(String name, NodeState nodeState) {
        return nodeState;
    }

    /**
     * Secure the after state of a child node such that it only provides
     * accessible child nodes and properties.
     * @param name       name of the child node
     * @param nodeState  after state of the child node
     * @return  secured after state
     */
    @Nonnull
    protected NodeState secureAfter(String name, NodeState nodeState) {
        return nodeState;
    }

    //------------------------------------------------------------< Validator >---

    @Override
    public void enter(NodeState before, NodeState after) throws CommitFailedException {
    }

    @Override
    public void leave(NodeState before, NodeState after) throws CommitFailedException {
    }

    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        if (canRead(beforeTree, null, afterTree, after)) {
            validator.propertyAdded(after);
        }
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        if (canRead(beforeTree, before, afterTree, after)) {
            validator.propertyChanged(before, after);
        }
    }

    @Override
    public void propertyDeleted(PropertyState before) throws CommitFailedException {
        if (canRead(beforeTree, before, afterTree, null)) {
            validator.propertyDeleted(before);
        }
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) throws CommitFailedException {
        if (canRead(beforeTree.getChild(name), afterTree.getChild(name))) {
            Validator childValidator = validator.childNodeAdded(name, secureAfter(name, after));
            return childValidator == null
                ? null
                : create(beforeTree.getChild(name), afterTree.getChild(name), childValidator);
        } else {
            return null;
        }
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after)
            throws CommitFailedException {
        Validator childValidator = validator.childNodeChanged(
                name, secureBefore(name, before), secureAfter(name, after));
        return childValidator == null
            ? null
            : create(beforeTree.getChild(name), afterTree.getChild(name), childValidator);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) throws CommitFailedException {
        if (canRead(beforeTree.getChild(name), afterTree.getChild(name))) {
            Validator childValidator = validator.childNodeDeleted(name, secureBefore(name, before));
            return childValidator == null
                ? null
                : create(beforeTree.getChild(name), afterTree.getChild(name), childValidator);
        } else {
            return null;
        }
    }

}
