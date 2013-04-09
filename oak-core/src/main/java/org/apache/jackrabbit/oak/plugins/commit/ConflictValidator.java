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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.oak.api.Type.STRINGS;

/**
 * {@link Validator} which checks the presence of conflict markers
 * in the tree in fails the commit if any are found.
 *
 * @see AnnotatingConflictHandler
 */
public class ConflictValidator extends DefaultValidator {

    private final ConflictValidator parent;

    private final String name;

    public ConflictValidator() {
        this.parent = null;
        this.name = null;
    }

    private ConflictValidator(ConflictValidator parent, String name) {
        this.parent = null;
        this.name = name;
    }

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
        return new ConflictValidator(this, name);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before, NodeState after) {
        return new ConflictValidator(this, name);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) {
        return null;
    }

    private void failOnMergeConflict(PropertyState property) throws CommitFailedException {
        if (JcrConstants.JCR_MIXINTYPES.equals(property.getName())) {
            assert property.isArray();
            for (String v : property.getValue(STRINGS)) {
                if (NodeTypeConstants.MIX_REP_MERGE_CONFLICT.equals(v)) {
                    throw new CommitFailedException(
                            "State", 1, "Unresolved conflicts in " + getPath());
                }
            }
        }
    }

    private String getPath() {
        if (parent == null) {
            return "/";
        } else {
            return parent.getPath(name);
        }
    }

    private String getPath(String name) {
        if (parent == null) {
            return "/" + name;
        } else {
            return parent.getPath(this.name) + "/" + name;
        }
    }

}
