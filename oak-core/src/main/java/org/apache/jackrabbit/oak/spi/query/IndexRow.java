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
package org.apache.jackrabbit.oak.spi.query;

import org.apache.jackrabbit.oak.api.PropertyValue;

/**
 * A row returned by the index.
 */
public interface IndexRow {
    /**
     * Marks if the row is virtual and behavior of {@code getPath} is undefined. The implementation may
     * choose to return {@code null} or empty string. User of a virtual row should now rely of value of
     * {@code getPath} returned from virtual rows.
     * @return if path is available for the current row
     */
    boolean isVirtualRow();

    /**
     * The path of the node, if available.
     *
     * @return the path
     */
    String getPath();

    /**
     * The value of the given property, if available. This might be a property
     * of the given node, or a pseudo-property (a property that is only
     * available in the index but not in the node itself, such as "jcr:score").
     *
     * @param columnName the column name
     * @return the value, or null if not available
     */
    PropertyValue getValue(String columnName);

}
