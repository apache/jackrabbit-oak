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

package org.apache.jackrabbit.oak.plugins.index.lucene.hybrid;

public interface ReaderRefreshPolicy {
    ReaderRefreshPolicy NEVER = new ReaderRefreshPolicy() {
        @Override
        public void refreshOnReadIfRequired(Runnable refreshCallback) {
            //Never refresh
        }

        @Override
        public void refreshOnWriteIfRequired(Runnable refreshCallback) {
            //Never refresh
        }
    };

    /**
     * This would be invoked before any query is performed
     * to provide a chance for IndexNode to refresh the readers
     *
     * <p>The index may or may not be updated when this method
     * is invoked
     *
     * @param refreshCallback callback to refresh the readers
     */
    void refreshOnReadIfRequired(Runnable refreshCallback);

    /**
     * This would invoked after some writes have been performed
     * and as a final step refresh request is being made.
     *
     * <p>Any time its invoked it can be assumed that index has been
     * updated
     *
     * @param refreshCallback callback to refresh the readers
     */
    void refreshOnWriteIfRequired(Runnable refreshCallback);
}
