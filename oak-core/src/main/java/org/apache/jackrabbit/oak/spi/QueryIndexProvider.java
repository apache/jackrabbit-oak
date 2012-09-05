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
package org.apache.jackrabbit.oak.spi;

import java.util.List;
import org.apache.jackrabbit.mk.api.MicroKernel;

/**
 * A mechanism to index data. Indexes might be added or removed at runtime,
 * possibly by changing content in the repository. The provider knows about the
 * indexes available at a given time.
 */
public interface QueryIndexProvider {

    /**
     * Get the currently configured indexes for the given MicroKernel instance.
     *
     * @param mk the MicroKernel instance
     * @return the list of indexes
     */
    List<? extends QueryIndex> getQueryIndexes(MicroKernel mk);

}
