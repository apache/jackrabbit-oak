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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.Map;

/**
 * Receives callbacks from the {@link NodeDocumentSweeper} on what updates
 * are required for the sweep ({@link #sweepUpdate(Map)} and required
 * invalidation of documents.
 */
interface NodeDocumentSweepListener {

    /**
     * Called for a batch of sweep updates that should be performed.
     *
     * @param updates the update operations. The keys in the map are the paths
     *                of the documents to update.
     * @throws DocumentStoreException if the operation fails.
     */
    void sweepUpdate(Map<String, UpdateOp> updates) throws DocumentStoreException;

}
