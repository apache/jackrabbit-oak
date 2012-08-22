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
import java.util.Set;

import javax.annotation.Nonnull;

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
 * TODO It *should* define an API for managing indexes (CRUD ops)
 * </p>
 * 
 * <p>
 * TODO Document simple node properties to create an index type
 * </p>
 * </p>
 */
public interface IndexManager extends Closeable {

    /**
     * Creates an index by passing the {@link IndexDefinition} to the registered
     * {@link IndexFactory}(es)
     * 
     * @param indexDefinition
     */
    void registerIndex(IndexDefinition... indexDefinition);

    void registerIndexFactory(IndexFactory factory);

    void init();

    @Nonnull
    Set<IndexDefinition> getIndexes();
}
