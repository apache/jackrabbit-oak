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
package org.apache.jackrabbit.oak.api.jmx;

import org.osgi.annotation.versioning.ProviderType;

/**
 * An MBean that provides the inference configuration.
 */
@ProviderType
public interface InferenceMBean {

    String TYPE = "Inference";

    /**
     * Get the inference configuration as a Json string.
     */
    String getConfigJson();

    /**
     * Get the inference configuration as a Json string.
     */
    String getConfigNodeStateJson();
}
