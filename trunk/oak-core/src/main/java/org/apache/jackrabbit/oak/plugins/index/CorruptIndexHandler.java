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

import java.util.Calendar;

public interface CorruptIndexHandler {
    CorruptIndexHandler NOOP = new CorruptIndexHandler() {
        @Override
        public boolean skippingCorruptIndex(String async, String indexPath, Calendar corruptSince) {
            return false;
        }

        @Override
        public void indexUpdateFailed(String async, String indexPath, Exception e) {

        }
    };

    /**
     * Callback method to inform handler that a corrupt index has been skipped
     *
     * @param async async name
     * @param indexPath corrupt index path
     * @param corruptSince time since index is corrupt
     * @return true if warning is logged for skipped indexing
     */
    boolean skippingCorruptIndex(String async, String indexPath, Calendar corruptSince);

    void indexUpdateFailed(String async, String indexPath, Exception e);
}
