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
package org.apache.jackrabbit.oak.osgi;

import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * This IndexEditor provider combines all index editors of all available OSGi
 * IndexEditor providers.
 */
public class OsgiIndexEditorProvider extends
        AbstractServiceTracker<IndexEditorProvider> implements IndexEditorProvider {

    public OsgiIndexEditorProvider() {
        super(IndexEditorProvider.class);
    }

    @Override
    public Editor getIndexEditor(String type, NodeBuilder builder) {
        IndexEditorProvider composite = CompositeIndexEditorProvider
                .compose(getServices());
        if (composite == null) {
            return null;
        }
        return composite.getIndexEditor(type, builder);
    }

}
