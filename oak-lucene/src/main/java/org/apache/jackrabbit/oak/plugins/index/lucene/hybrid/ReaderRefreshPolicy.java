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
        public boolean shouldRefresh() {
            return false;
        }

        @Override
        public void updated() {

        }
    };

    /**
     * Returns  true if refresh is to be done and
     * resets the internal state. The caller which
     * gets the true answer would be responsible for
     * refreshing the readers.
     *
     * <p>For e.g. once updated the first call to
     * this method would return true and subsequent
     * calls return false
     *
     * @return true if refresh is to be done
     */
    boolean shouldRefresh();

    /**
     * Invoked when index gets updated
     */
    void updated();
}
