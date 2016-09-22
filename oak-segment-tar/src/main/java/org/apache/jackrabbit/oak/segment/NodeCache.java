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

package org.apache.jackrabbit.oak.segment;

import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

/**
 * Partial mapping of {@code String} keys to values of type {@link RecordId}. The mappings
 * can further be associated with a cost, which is a metric for the cost occurring when the
 * given mapping is lost. Higher values represent higher costs.
 */
public interface NodeCache {

    /**
     * Add a mapping from {@code key} to {@code value} with a given {@code cost}.
     */
    void put(@Nonnull String stableId, @Nonnull RecordId recordId, byte cost);

    /**
     * @return  The mapping for {@code key}, or {@code null} if none.
     */
    @CheckForNull
    RecordId get(@Nonnull String stableId);
}
