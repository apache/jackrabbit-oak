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
package org.apache.jackrabbit.oak.plugins.version;

import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.commit.CompositeEditor;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.commit.EditorProvider;
import org.apache.jackrabbit.oak.spi.commit.SubtreeEditor;
import org.apache.jackrabbit.oak.spi.commit.VisibleEditor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import static org.apache.jackrabbit.JcrConstants.JCR_SYSTEM;
import static org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants.JCR_VERSIONSTORAGE;

/**
 * A provider creating two editors: {@link VersionEditor}
 * {@link VersionStorageEditor}.
 * <p>
 * Historically, it has been used to initalize the Jcr repository. Now, the
 * more general {@link VersionHook} should be passed there using the {@code with()}
 * method.
 */
class VersionEditorProvider implements EditorProvider {

    @Override
    public Editor getRootEditor(
            NodeState before, NodeState after,
            NodeBuilder builder, CommitInfo info) {
        if (!builder.hasChildNode(JCR_SYSTEM)) {
            return null;
        }
        NodeBuilder system = builder.child(JCR_SYSTEM);
        if (!system.hasChildNode(JCR_VERSIONSTORAGE)) {
            return null;
        }
        NodeBuilder versionStorage = system.child(JCR_VERSIONSTORAGE);
        return new VisibleEditor(new CompositeEditor(
                new VersionEditor(versionStorage, builder, info),
                new SubtreeEditor(
                        new VersionStorageEditor(versionStorage, builder),
                            JCR_SYSTEM, JCR_VERSIONSTORAGE)));
    }

}
