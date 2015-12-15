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

import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.apache.lucene.document.Field;

/**
 * Implementations of this interface would get callbacks while indexing documents. It's the responsibility
 * of the implementation to exit as early as possible if it doesn't care about the document being indexed.
 */
public interface IndexFieldProvider {
    /**
     * This method would get called while indexing property changes. The method would be called once for each property
     * that is changed.
     *
     * @param path path of the document being indexed
     * @param propertyName property name (including relative path, if any) of the changed property
     * @param document {@link NodeState} of the document being indexed
     * @param property {@link PropertyState} of changed property
     * @param indexDefinition {@link NodeState} of index definition
     * @return {@link Iterable} of fields that are to be added to {@link org.apache.lucene.document.Document} being prepared
     */
    Iterable<Field> getAugmentedFields(final String path, final String propertyName,
                                   final NodeState document, final PropertyState property,
                                   final NodeState indexDefinition);
}
