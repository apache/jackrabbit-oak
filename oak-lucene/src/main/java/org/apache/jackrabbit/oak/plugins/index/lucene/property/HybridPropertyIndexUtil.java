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

package org.apache.jackrabbit.oak.plugins.index.lucene.property;

class HybridPropertyIndexUtil {
    /**
     * Node name under which all property indexes are created
     */
    static final String PROPERTY_INDEX = ":property-index";

    /**
     * Property name referring to 'head' bucket
     */
    static final String PROP_HEAD_BUCKET = "head";

    /**
     * Property name referring to 'previous' bucket
     */
    static final String PROP_PREVIOUS_BUCKET = "previous";

    static String getNodeName(String propertyRelativePath) {
        return propertyRelativePath.replace('/', '_');
    }
}
