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

package org.apache.jackrabbit.oak.plugins.index;

import java.io.IOException;

import org.jetbrains.annotations.Nullable;
import org.osgi.annotation.versioning.ProviderType;

@ProviderType
public interface IndexInfoService {

    /**
     * Returns {@code IndexInfo} for all the indexes present in
     * the repository
     */
    Iterable<IndexInfo> getAllIndexInfo();

    /**
     * Returns {@code IndexInfo} for index at given path
     *
     * @param indexPath path repository
     *
     * @return indexInfo for the index or null if there is no index node
     * found at given path
     */
    @Nullable
    IndexInfo getInfo(String indexPath) throws IOException;

    /**
     * Determined if the index is valid and usable. If the index is corrupt
     * then it returns false
     */
    boolean isValid(String indexPath) throws IOException;
}
