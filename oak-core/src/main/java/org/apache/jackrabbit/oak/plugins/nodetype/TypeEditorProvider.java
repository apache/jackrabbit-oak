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
import static org.apache.jackrabbit.oak.api.Type.NAME;
import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.MISSING_NODE;
import static org.apache.jackrabbit.oak.plugins.nodetype.NodeTypeConstants.JCR_NODE_TYPES;

import java.util.Collections;
import java.util.Set;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorDiff;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

@Component
@Service(EditorProvider.class)
public class TypeEditorProvider implements EditorProvider {

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

        Set<String> modifiedTypes = Collections.emptySet();
        TypeRegistration registration = new TypeRegistration();
        afterTypes.compareAgainstBaseState(beforeTypes, registration);
        if (registration.isModified()) {
            afterTypes = registration.apply(builder);
            modifiedTypes = registration.getModifiedTypes(beforeTypes);
        }

        String primary = after.getName(JCR_PRIMARYTYPE);
        Iterable<String> mixins = after.getNames(JCR_MIXINTYPES);

        if (primary == null && afterTypes.hasChildNode("rep:root")) {
            // no primary type on the root node, set the hardcoded default
            primary = "rep:root";
            builder.setProperty(JCR_PRIMARYTYPE, primary, NAME);
        }

        Editor editor = new VisibleEditor(
                new TypeEditor(strict, afterTypes, primary, mixins, builder));
        if (modifiedTypes.isEmpty()) {
            return editor;
        } else {
            // Some node types were modified, so scan the entire repository
            // to make sure that the modified type definitions still apply.
            // TODO: Only check the content that uses the modified node types.
            CommitFailedException exception =
                    EditorDiff.process(editor, MISSING_NODE, after);
            if (exception != null) {
                throw exception;
            }
            return null;
        }
    }

}
