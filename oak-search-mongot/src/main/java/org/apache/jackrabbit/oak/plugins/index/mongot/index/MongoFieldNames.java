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
package org.apache.jackrabbit.oak.plugins.index.mongot.index;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public final class MongoFieldNames {

    public static final String ID = "_id";
    public static final String PATH = "_path";
    public static final String PARENT = "_parent";
    public static final String ANCESTORS = "_ancestors";
    public static final String DEPTH = "_depth";
    public static final String PRIMARY_TYPE = "_primaryType";
    public static final String MIXIN_TYPES = "_mixinTypes";
    public static final String FULLTEXT = "_fulltext";
    public static final String SUGGEST = "_suggest";
    public static final String SPELLCHECK = "_spellcheck";
    public static final String NULL_PROPERTIES = "_nullProperties";
    public static final String NOT_NULL_PROPERTIES = "_notNullProperties";
    public static final String SYNC_TOKEN = "_syncToken";
    public static final String TYPED = "typed";
    public static final String ANALYZED = "analyzed";
    public static final String ORDERED = "ordered";
    public static final String FACET = "facet";
    public static final String RELATIVE_FULLTEXT = "relativeFulltext";
    public static final String DYNAMIC_BOOST = "dynamicBoost";
    public static final String DYNAMIC_BOOST_TOKENS = "dynamicBoostTokens";
    public static final String DYNAMIC_BOOST_SCORES = "dynamicBoostScores";

    private static final Base64.Encoder PROPERTY_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private MongoFieldNames() {
    }

    public static String encodeProperty(String propertyName) {
        return PROPERTY_ENCODER.encodeToString(propertyName.getBytes(StandardCharsets.UTF_8));
    }
}
