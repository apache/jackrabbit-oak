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
package org.apache.jackrabbit.oak.plugins.nodetype;

import static org.apache.jackrabbit.JcrConstants.JCR_MIXINTYPES;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_NODE_TYPES;

import java.util.Set;

import javax.jcr.RepositoryException;
import javax.jcr.nodetype.NoSuchNodeTypeException;
import javax.jcr.nodetype.NodeType;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.oak.plugins.nodetype.TypeEditor.ConstraintViolationCallback;
import org.apache.jackrabbit.oak.plugins.tree.factories.RootFactory;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.service.component.annotations.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Component(service = EditorProvider.class)
public class TypeEditorProvider implements EditorProvider {

    private static final Logger LOG = LoggerFactory.getLogger(TypeEditorProvider.class);

    private final boolean strict;

    public TypeEditorProvider(boolean strict) {
        this.strict = strict;
    }

    public TypeEditorProvider() {
        this(true);
    }

    @Override
    public Editor getRootEditor(
            NodeState before, NodeState after, NodeBuilder builder,
            CommitInfo info) throws CommitFailedException {
        NodeState beforeTypes =
                before.getChildNode(JCR_SYSTEM).getChildNode(JCR_NODE_TYPES);
        NodeState afterTypes =
                after.getChildNode(JCR_SYSTEM).getChildNode(JCR_NODE_TYPES);

        String primary = after.getName(JCR_PRIMARYTYPE);
        Iterable<String> mixins = after.getNames(JCR_MIXINTYPES);

        TypeRegistration registration = new TypeRegistration();
        afterTypes.compareAgainstBaseState(beforeTypes, registration);
        if (registration.isModified()) {
            ReadOnlyNodeTypeManager ntBefore = ReadOnlyNodeTypeManager.getInstance(RootFactory.createReadOnlyRoot(before), NamePathMapper.DEFAULT);
            ReadOnlyNodeTypeManager ntAfter = ReadOnlyNodeTypeManager.getInstance(RootFactory.createReadOnlyRoot(after), NamePathMapper.DEFAULT);

            afterTypes = registration.apply(builder);

            Set<String> modifiedTypes =
                    registration.getModifiedTypes(beforeTypes);
            if (!modifiedTypes.isEmpty()) {
                boolean modified = false;
                for (String t : modifiedTypes) {
                    boolean mod = !isTrivialChange(ntBefore, ntAfter, t);
                    modified = modified || mod;
                }

                if (!modified) {
                    LOG.info("Node type changes: " + modifiedTypes + " appear to be trivial, repository will not be scanned");
                }
                else {
                    
                    ConstraintViolationCallback callback = strict ? TypeEditor.THROW_ON_CONSTRAINT_VIOLATION : TypeEditor.WARN_ON_CONSTRAINT_VIOLATION;
                    
                    long start = System.currentTimeMillis();
                    // Some node types were modified, so scan the repository
                    // to make sure that the modified definitions still apply.
                    Editor editor = new VisibleEditor(new TypeEditor(
                            callback, modifiedTypes, afterTypes,
                            primary, mixins, builder));
                    LOG.info("Node type changes: " + modifiedTypes + " appear not to be trivial, starting repository scan");
                    CommitFailedException exception =
                            EditorDiff.process(editor, MISSING_NODE, after);
                    LOG.info("Node type changes: " + modifiedTypes + "; repository scan took " + (System.currentTimeMillis() - start)
                            + "ms" + (exception == null ? "" : "; failed with " + exception.getMessage()));
                    if (exception != null) {
                        throw exception;
                    }
                }
            }
        }
        
        ConstraintViolationCallback callback = strict ? TypeEditor.THROW_ON_CONSTRAINT_VIOLATION : TypeEditor.WARN_ON_CONSTRAINT_VIOLATION;

        return new VisibleEditor(new TypeEditor(
                callback, null, afterTypes, primary, mixins, builder));
    }

    private boolean isTrivialChange(ReadOnlyNodeTypeManager ntBefore, ReadOnlyNodeTypeManager ntAfter, String nodeType) {

        NodeType nb, na;

        try {
            nb = ntBefore.getNodeType(nodeType);
        } catch (NoSuchNodeTypeException ex) {
            LOG.info(nodeType + " not present in 'before' state");
            return true;
        } catch (RepositoryException ex) {
            LOG.info("getting node type", ex);
            return false;
        }

        try {
            na = ntAfter.getNodeType(nodeType);
        } catch (NoSuchNodeTypeException ex) {
            LOG.info(nodeType + " was removed");
            return false;
        } catch (RepositoryException ex) {
            LOG.info("getting node type", ex);
            return false;
        }

        NodeTypeDefDiff diff = NodeTypeDefDiff.create(nb, na);
        if (!diff.isModified()) {
            LOG.info("Node type " + nodeType + " was not changed");
            return true;
        } else if (diff.isTrivial()) {
            LOG.info("Node type change for " + nodeType + " appears to be trivial");
            return true;
        } else {
            LOG.info("Node type change for " + nodeType + " requires repository scan: " + diff);
            return false;
        }
    }
}
