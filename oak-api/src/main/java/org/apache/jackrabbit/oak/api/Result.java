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
     * Get the distinct selector names of all columns. The list is ordered as
     * selectors appear in the result. For columns without selector, an empty
     * entry (null) is used.
     * 
     * @return the distinct selector names
     */
    String[] getColumnSelectorNames();

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

    /**
     * Get the number of rows, if known. If the size is not known, -1 is
     * returned.
     * 
     * @param precision the required precision
     * @param max the maximum number that should be returned (Long.MAX_VALUE for
     *            unlimited). For EXACT, the cost of the operation is at most
     *            O(max). For approximations, the cost of the operation should
     *            be at most O(log max).
     * @return the (approximate) size. If an implementation does know the exact
     *         value, it returns it (even if the value is higher than max). If
     *         the implementation does not know the value, and the child node
     *         count is higher than max, it returns Long.MAX_VALUE.
     */
    long getSize(SizePrecision precision, long max);
    
    enum SizePrecision {
   
        /**
         * If the exact number is needed.
         */
        EXACT,
        
        /**
         * If a good, and secure estimate is needed (the actual number can be
         * lower or higher). This is supposed to be faster than exact count, but
         * slower than a fast approximation.
         */
        APPROXIMATION,
        
        /**
         * If a rough estimate is needed (the actual number can be lower or
         * higher). This is supposed to be faster than a good approximation. It
         * could be (for example) the expected cost of the query.
         */
        FAST_APPROXIMATION,
        
    }

}
