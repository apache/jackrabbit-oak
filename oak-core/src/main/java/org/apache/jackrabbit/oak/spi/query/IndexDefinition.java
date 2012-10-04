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

import java.util.Map;

import javax.annotation.Nonnull;

/**
 * Defines an index definition
 * 
 */
public interface IndexDefinition {

    String TYPE_PROPERTY_NAME = "type";

    String UNIQUE_PROPERTY_NAME = "unique";

    String INDEX_DATA_CHILD_NAME = ":data";

    /**
     * Get the unique index name. This is also the name of the index node.
     * 
     * @return the index name
     */
    @Nonnull
    String getName();

    /**
     * @return the index type
     */
    @Nonnull
    String getType();

    /**
     * @return the index path, including the name as the last segment
     */
    @Nonnull
    String getPath();

    /**
     * Whether each value may only appear once in the index.
     * 
     * @return true if unique
     */
    boolean isUnique();

    /**
     * @return the index properties
     */
    @Nonnull
    Map<String, String> getProperties();

}
