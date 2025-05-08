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

import org.apache.jackrabbit.oak.api.StrictPathRestriction;
import org.jetbrains.annotations.NotNull;

public interface QueryLimits {

    long getLimitInMemory();

    long getLimitReads();

    boolean getFullTextComparisonWithoutIndex();

    /**
     * See OAK-10532. This method is used for backward compatibility
     * (bug compatibility) only.
     *
     * @return true, except when backward compatibility for OAK-10532 is enabled
     */
    default boolean getImprovedIsNullCost() {
        return true;
    }

    /**
     * See OAK-11214. This method is used for backward compatibility
     * (bug compatibility) only.
     *
     * @return true, except when backward compatibility for OAK-11214 is enabled
     */
    default public boolean getOptimizeInRestrictionsForFunctions() {
        return true;
    }

    boolean getFailTraversal();

    default String getStrictPathRestriction() {
        return StrictPathRestriction.DISABLE.name();
    }

    /**
     * Retrieve the java package names / full qualified class names which should be
     * ignored when finding the class starting a query
     * @return the name of the packages / full qualified class names
     */
    default @NotNull String[] getIgnoredClassNamesInCallTrace() {
        return new String[] {};
    }

}
