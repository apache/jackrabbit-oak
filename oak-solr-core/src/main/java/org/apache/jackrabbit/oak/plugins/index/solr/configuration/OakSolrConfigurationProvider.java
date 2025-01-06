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
package org.apache.jackrabbit.oak.plugins.index.solr.configuration;

import org.jetbrains.annotations.NotNull;

/**
 * A provider for {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration}s
 * <p>
 * @deprecated Solr support is deprecated and will be removed in a future version of Oak; see <a href=https://issues.apache.org/jira/browse/OAK-11314 target=_blank>Jira ticket OAK-11314</a> for more information.
 */
@Deprecated(forRemoval=true, since="1.74.0")
public interface OakSolrConfigurationProvider {

    /**
     * Provides a Solr configuration for Oak
     *
     * @return a {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfiguration}
     */
    @NotNull
    OakSolrConfiguration getConfiguration();
}
