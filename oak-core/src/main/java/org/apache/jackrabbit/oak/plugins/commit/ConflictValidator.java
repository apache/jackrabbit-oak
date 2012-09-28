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
package org.apache.jackrabbit.oak.plugins.commit;

import javax.jcr.InvalidItemStateException;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.CoreValue;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.type.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * {@link Validator} which checks the presence of conflict markers
 * in the tree in fails the commit if any are found.
 *
 * @see AnnotatingConflictHandler
 */
public class ConflictValidator extends DefaultValidator {
    @Override
    public void propertyAdded(PropertyState after) throws CommitFailedException {
        failOnMergeConflict(after);
    }

    @Override
    public void propertyChanged(PropertyState before, PropertyState after)
            throws CommitFailedException {
        failOnMergeConflict(after);
    }

    @Override
    public Validator childNodeAdded(String name, NodeState after) {
        return this;
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) {
        return this;
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) {
        return this;
    }

    private static void failOnMergeConflict(PropertyState property) throws CommitFailedException {
        if (JcrConstants.JCR_MIXINTYPES.equals(property.getName())) {
            assert property.isArray();
            for (CoreValue v : property.getValues()) {
                if (NodeTypeConstants.MIX_REP_MERGE_CONFLICT.equals(v.getString())) {
                    throw new CommitFailedException(new InvalidItemStateException("Item has unresolved conflicts"));
                }
            }
        }
    }
}
