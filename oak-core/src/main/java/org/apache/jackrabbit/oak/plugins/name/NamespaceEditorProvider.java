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
package org.apache.jackrabbit.oak.plugins.name;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.namespace.NamespaceConstants.REP_NAMESPACES;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.SubtreeEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.osgi.service.component.annotations.Component;

/**
 * Validator service that checks that all node and property names as well
 * as any name values are syntactically valid and that any namespace prefixes
 * are properly registered.
 */
@Component(service = EditorProvider.class)
public class NamespaceEditorProvider implements EditorProvider {

    @Override
    public Editor getRootEditor(
            NodeState before, NodeState after, NodeBuilder builder,
            CommitInfo info) throws CommitFailedException {
        return new SubtreeEditor(new NamespaceEditor(before, builder), JCR_SYSTEM, REP_NAMESPACES);
    }

}
