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
package org.apache.jackrabbit.oak.plugins.index;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.jackrabbit.oak.spi.whiteboard.AbstractServiceTracker;

/**
 * Dynamic {@link IndexEditorProvider} based on the available
 * whiteboard services.
 */
public class WhiteboardIndexEditorProvider
        extends AbstractServiceTracker<IndexEditorProvider>
        implements IndexEditorProvider {

    public WhiteboardIndexEditorProvider() {
        super(IndexEditorProvider.class);
    }

    @Override
    public Editor getIndexEditor(@Nonnull String type, @Nonnull NodeBuilder builder,
            @Nonnull NodeState root, @Nonnull IndexUpdateCallback callback)
            throws CommitFailedException {
        IndexEditorProvider composite = CompositeIndexEditorProvider
                .compose(getServices());
        if (composite == null) {
            return null;
        }
        return composite.getIndexEditor(type, builder, root, callback);
    }

}
