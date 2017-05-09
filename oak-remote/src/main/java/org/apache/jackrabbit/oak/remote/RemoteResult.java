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

package org.apache.jackrabbit.oak.remote;

/**
 * A single search result.
 */
public interface RemoteResult {

    /**
     * The value attached to this search result at the specified column.
     *
     * @param column Name of the column.
     * @return An instance of {@code RemoteValue}.
     */
    RemoteValue getColumnValue(String column);

    /**
     * Read the path of the node associated with the specified selector.
     *
     * @param selector The name of the selector.
     * @return A node path.
     */
    String getSelectorPath(String selector);

}
