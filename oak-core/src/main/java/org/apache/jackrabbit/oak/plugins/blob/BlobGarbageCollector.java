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
package org.apache.jackrabbit.oak.plugins.blob;

import java.util.List;

/**
 * Interface for blob garbage collector
 */
public interface BlobGarbageCollector {

    /**
     * Marks garbage blobs from the passed node store instance.
     * Collects them only if markOnly is false.
     *
     * @param markOnly whether to only mark references and not sweep in the mark and sweep operation.
     * @throws Exception the exception
     */
    void collectGarbage(boolean markOnly) throws Exception;
    
    /**
     * Retuns the list of stats
     * 
     * @return stats
     * @throws Exception
     */
    List<GarbageCollectionRepoStats> getStats() throws Exception;
}
