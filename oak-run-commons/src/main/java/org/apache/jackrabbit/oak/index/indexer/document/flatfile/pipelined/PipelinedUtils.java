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
package org.apache.jackrabbit.oak.index.indexer.document.flatfile.pipelined;

public class PipelinedUtils {
    /**
     * <p>Format a percentage as a string with 2 decimal places. For instance:
     * <code>formatAsPercentage(52, 1000)</code> returns <code>"5.20"</code>.</p>
     */
    public static String formatAsPercentage(long numerator, long denominator) {
        if (denominator == 0) {
            return "N/A";
        } else {
            return String.format("%1.2f", (100.0 * numerator) / denominator);
        }
    }

}
