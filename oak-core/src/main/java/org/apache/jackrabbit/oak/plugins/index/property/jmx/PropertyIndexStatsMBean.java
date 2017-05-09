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

package org.apache.jackrabbit.oak.plugins.index.property.jmx;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

import org.apache.jackrabbit.oak.api.jmx.Description;
import org.apache.jackrabbit.oak.api.jmx.Name;

public interface PropertyIndexStatsMBean {
    String TYPE = "PropertyIndexStats";

    @Description("Determines statistics related to specific property index which can be used to optimize property index " +
            "definition. Various limits below are provided to ensure that estimation logic does not consume too much " +
            "resources. If any limits are reached then report would not be considered conclusive and would not have " +
            "paths set determined")
    CompositeData getStatsForSpecificIndex(@Name("indexPath")
                           @Description("Index path for which stats are to be determined")
                           String path,
                           @Name("maxValueCount")
                           @Description("Maximum number of values to examine. E.g. 100. Stats calculation would " +
                                   "break out after this limit")
                           int maxValueCount,
                           @Description("Maximum depth to examine. E.g. 5. Stats calculation would " +
                                   "break out after this limit")
                           @Name("maxDepth")
                           int maxDepth,
                           @Description("Maximum number of unique paths to examine. E.g. 100. Stats " +
                                   "calculation would break out after this limit")
                           @Name("maxPathCount")
                           int maxPathCount
    ) throws OpenDataException;

    @Description("Determines statistics related to property index for all property index under given path. Various limits " +
            "below are provided to ensure that estimation logic does not consume too much " +
            "resources. If any limits are reached then report would not be considered conclusive and would not have " +
            "paths set determined")
    TabularData getStatsForAllIndexes(
                           @Name("path")
                           @Description("Path under which all indexes are to be examined like '/'. Path should not " +
                                   "include oak:index")
                           String path,
                           @Name("maxValueCount")
                           @Description("Maximum number of values to examine. E.g. 100. Stats calculation would " +
                                   "break out after this limit")
                           int maxValueCount,
                           @Description("Maximum depth to examine. E.g. 5. Stats calculation would " +
                                   "break out after this limit")
                           @Name("maxDepth")
                           int maxDepth,
                           @Description("Maximum number of unique paths to examine. E.g. 100. Stats " +
                                   "calculation would break out after this limit")
                           @Name("maxPathCount")
                           int maxPathCount
    ) throws OpenDataException;


}
