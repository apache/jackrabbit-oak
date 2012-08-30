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
package org.apache.jackrabbit.oak.api;

/**
 * A result from executing a query.
 */
public interface Result {

    /**
     * Get the list of column names.
     *
     * @return the column names
     */
    String[] getColumnNames();

    /**
     * Get the list of selector names.
     *
     * @return the selector names
     */
    String[] getSelectorNames();

    /**
     * Get the rows.
     *
     * @return the rows
     */
    Iterable<? extends ResultRow> getRows();

    /**
     * Get the number of rows, if known. If the size is not known, -1 is
     * returned.
     *
     * @return the size or -1 if unknown
     */
    long getSize();

}
