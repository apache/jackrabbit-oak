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

package org.apache.jackrabbit.oak.plugins.index.lucene;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.jetbrains.annotations.Nullable;

/**
 * Callback to be invoked for each indexable property change
 */
public interface PropertyUpdateCallback {

    /**
     * Invoked upon any change in property either added, updated or removed.
     * Implementation can determine if property is added, updated or removed based
     * on whether before or after is null
     *
     * @param nodePath path of node for which is to be indexed for this property change
     * @param propertyRelativePath relative path of the property wrt the indexed node
     * @param pd property definition associated with the property to be indexed
     * @param before before state. Is null when property is added. For other cases its not null
     * @param after after state of the property. Is null when property is removed. For other cases its not null
     */
    void propertyUpdated(String nodePath, String propertyRelativePath, PropertyDefinition pd,
                         @Nullable  PropertyState before, @Nullable PropertyState after);

    /**
     * Invoked after editor has traversed all the changes
     *
     * @throws CommitFailedException in case some validation fails
     */
    void done() throws CommitFailedException;

}
