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
package org.apache.jackrabbit.oak.plugins.index.elastic.query.inference;

public interface InferenceConstants {
    String ENABLED = "enabled";
    String ENRICHER_CONFIG = "enricherConfig";
    String TYPE = "type";
    String DEFAULT_OAK_INDEX_INFERENCE_CONFIG_PATH = "/oak:index/:inferenceConfig";
    String VECTOR_SPACES = ":vectorSpaces";
    String VECTOR = "vector";
    String ENRICH_NODE = ":enrich";
    String ENRICHER_STATUS_DATA = "enricherStatusData";
    String ENRICHER_STATUS_MAPPING = "enricherStatusMapping";
    String DEFAULT_ENVIRONMENT_VARIABLE_PREFIX = "$";
    String INFERENCE_ENVIRONMENT_VARIABLE_PREFIX = System.getProperty("org.apache.jackrabbit.oak.plugins.index.elastic.query.inference", DEFAULT_ENVIRONMENT_VARIABLE_PREFIX);
    String DEFAULT_ENVIRONMENT_VARIABLE_VALUE = "";

}
