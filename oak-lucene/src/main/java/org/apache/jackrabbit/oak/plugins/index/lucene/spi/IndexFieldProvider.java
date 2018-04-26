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
package org.apache.jackrabbit.oak.plugins.index.lucene.spi;

import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Field;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Set;

/**
 * Implementations of this interface would get callbacks while indexing documents. It's the responsibility
 * of the implementation to exit as early as possible if it doesn't care about the document being indexed.
 */
public interface IndexFieldProvider {
    /**
     * Implementation which doesn't do anything useful... yet, abides with the contract.
     */
    IndexFieldProvider DEFAULT = new IndexFieldProvider() {
        @Override
        public Iterable<Field> getAugmentedFields(String path, NodeState document, NodeState indexDefinition) {
            return Collections.EMPTY_LIST;
        }

        @Override
        public Set<String> getSupportedTypes() {
            return Collections.EMPTY_SET;
        }
    };

    /**
     * This method would get called while indexing a document.
     *
     * @param path path of the document being indexed
     * @param document {@link NodeState} of the document being indexed
     * @param indexDefinition {@link NodeState} of index definition
     * @return {@link Iterable} of fields that are to be added to {@link org.apache.lucene.document.Document} being prepared
     */
    @Nonnull
    Iterable<Field> getAugmentedFields(String path, NodeState document, NodeState indexDefinition);

    /**
     * This method is used to find which node types are supported by the implementation. Based, on the index
     * definition being used to index the document, only those implementations would get callback to
     * {@link IndexFieldProvider#getAugmentedFields} which declare a matching node type. Note, node types are
     * exact matches and do not support inheritance.
     * @return {@link Set} of types supported by the implementation
     */
    @Nonnull
    Set<String> getSupportedTypes();
}
