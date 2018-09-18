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

import static org.apache.jackrabbit.oak.api.Type.STRINGS;
import static org.apache.jackrabbit.oak.commons.PathUtils.concat;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.MIX_REP_MERGE_CONFLICT;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;
import org.apache.jackrabbit.oak.spi.commit.DefaultValidator;
import org.apache.jackrabbit.oak.spi.commit.Validator;
import org.apache.jackrabbit.oak.spi.state.ChildNodeEntry;
import org.apache.jackrabbit.oak.spi.state.ConflictType;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * {@link Validator} which checks the presence of conflict markers
 * in the tree in fails the commit if any are found.
 *
 * @see AnnotatingConflictHandler
 */
public class ConflictValidator extends DefaultValidator {
    private static Logger log = LoggerFactory.getLogger(ConflictValidator.class);

    /**
     * Current processed path, or null if the debug log is not enabled at the
     * beginning of the call. The null check will also be used to verify if a
     * debug log will be needed or not
     */
    private final String path;

    private NodeState after;

    @Deprecated
    public ConflictValidator(Tree parentAfter) {
        this();
    }

    ConflictValidator() {
        this.path = "/";
    }

    private ConflictValidator(String path, String name) {
        this.path = concat(path, name);
    }

    @Override
    public void enter(NodeState before, NodeState after)
            throws CommitFailedException {
        this.after = after;
    }

    @Override
    public void leave(NodeState before, NodeState after)
            throws CommitFailedException {
        this.after = null;
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
        return new ConflictValidator(path, name);
    }

    @Override
    public Validator childNodeChanged(String name, NodeState before,
            NodeState after) {
        return new ConflictValidator(path, name);
    }

    @Override
    public Validator childNodeDeleted(String name, NodeState before) {
        return null;
    }

    private void failOnMergeConflict(PropertyState property) throws CommitFailedException {
        if (JcrConstants.JCR_MIXINTYPES.equals(property.getName())) {
            assert property.isArray();
            for (String v : property.getValue(STRINGS)) {
                if (MIX_REP_MERGE_CONFLICT.equals(v)) {

                    CommitFailedException ex = new CommitFailedException(
                            CommitFailedException.STATE, 1, "Unresolved conflicts in " + path);

                    //Conflict details are not made part of ExceptionMessage instead they are
                    //logged. This to avoid exposing property details to the caller as it might not have
                    //permission to access it
                    if (log.isDebugEnabled()) {
                        log.debug(getConflictMessage(), ex);
                    }
                    throw ex;
                }
            }
        }
    }

    private String getConflictMessage() {
        StringBuilder sb = new StringBuilder("Commit failed due to unresolved conflicts in ");
        sb.append(path);
        sb.append(" = {");

        for (ChildNodeEntry conflict : after.getChildNode(NodeTypeConstants.REP_OURS).getChildNodeEntries()) {
            ConflictType ct = ConflictType.fromName(conflict.getName());
            NodeState node = conflict.getNodeState();
            sb.append(ct.getName()).append(" = {");
            if (ct.effectsNode()) {
                sb.append(getChildNodeNamesAsString(node));
            } else {
                for (PropertyState ps : node.getProperties()) {
                    PropertyState ours = null, theirs = null;
                    switch (ct) {
                        case DELETE_CHANGED_PROPERTY:
                            ours = null;
                            theirs = ps;
                            break;
                        case ADD_EXISTING_PROPERTY:
                        case CHANGE_CHANGED_PROPERTY:
                            ours = ps;
                            theirs = after.getProperty(ps.getName());
                            break;
                        case CHANGE_DELETED_PROPERTY:
                            ours = ps;
                            theirs = null;
                            break;
                    }

                    sb.append(ps.getName())
                            .append(" = {")
                            .append(toString(ours))
                            .append(',')
                            .append(toString(theirs))
                            .append('}');

                    sb.append(',');
                }
                sb.deleteCharAt(sb.length() - 1);
            }

            sb.append("},");
        }

        sb.deleteCharAt(sb.length() - 1);
        sb.append('}');
        return sb.toString();
    }

    private static String getChildNodeNamesAsString(NodeState ns) {
        return Joiner.on(',').join(ns.getChildNodeNames());
    }

    private static String toString(PropertyState ps) {
        if (ps == null) {
            return "<N/A>";
        }

        final Type<?> type = ps.getType();
        if (type.isArray()) {
            return "<ARRAY>";
        }
        if (Type.BINARY == type) {
            return "<BINARY>";
        }

        String value = ps.getValue(Type.STRING);

        //Trim the value so as to not blowup diff message
        if (Type.STRING == type && value.length() > 10) {
            value = value.substring(0, 10) + "...";
        }

        return value;
    }

}
