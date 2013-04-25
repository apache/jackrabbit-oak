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
package org.apache.jackrabbit.oak.plugins.index;

import javax.annotation.CheckForNull;

import org.apache.jackrabbit.oak.spi.commit.Editor;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Extension point for plugging in different kinds of IndexEditor providers.
 * 
 * @see IndexEditor
 */
public interface IndexEditorProvider {

    /**
     * 
     * Each provider knows how to produce a certain type of index. If the
     * <code>type</code> param is of an unknown value, the provider is expected
     * to return {@code null}.
     * 
     * <p>
     * The <code>builder</code> must points to the index definition node
     * under which the indexer is expected to store the index content.
     * </p>
     * 
     * @param type
     *            the index type
     * @param builder
     *            the node state builder of the index definition node that
     *            will be used for updates
     * @return index update editor, or {@code null} if type is unknown
     */
    @CheckForNull
    Editor getIndexEditor(String type, NodeBuilder builder);
}
