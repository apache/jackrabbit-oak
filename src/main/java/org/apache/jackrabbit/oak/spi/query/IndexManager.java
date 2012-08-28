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
package org.apache.jackrabbit.oak.spi.query;

import java.io.Closeable;
import java.util.List;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.spi.commit.CommitEditor;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * <p>
 * Index Manager keeps track of all the available indexes.
 * </p>
 * 
 * <p>
 * As a configuration reference it will use the index definitions nodes at
 * {@link IndexUtils#DEFAULT_INDEX_HOME}.
 * </p>
 * 
 * <p>
 * TODO Document simple node properties to create an index type
 * </p>
 * </p>
 */
public interface IndexManager extends CommitEditor, Closeable {

    void registerIndexFactory(IndexFactory... factory);

    void unregisterIndexFactory(IndexFactory factory);

    /**
     * @return the index with the given name
     */
    @CheckForNull
    Index getIndex(String name, NodeState nodeState);

    /**
     * @return the index with the given definition
     */
    @CheckForNull
    public Index getIndex(IndexDefinition definition);

    /**
     * @return the existing index definitions
     */
    @Nonnull
    List<IndexDefinition> getIndexDefinitions(NodeState nodeState);
}
