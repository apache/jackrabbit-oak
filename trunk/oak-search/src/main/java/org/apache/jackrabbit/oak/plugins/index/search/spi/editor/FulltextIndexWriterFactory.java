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

package org.apache.jackrabbit.oak.plugins.index.search.spi.editor;

import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;

/**
 * Factory class for {@link FulltextIndexWriter}s
 */
public interface FulltextIndexWriterFactory<D> {

    /**
     * create a new index writer instance
     * @param definition the index definition
     * @param definitionBuilder the node builder associated with the index definition
     * @param reindex whether or not reindex should be performed
     * @return an index writer
     */
    FulltextIndexWriter<D> newInstance(IndexDefinition definition, NodeBuilder definitionBuilder, boolean reindex);

}
