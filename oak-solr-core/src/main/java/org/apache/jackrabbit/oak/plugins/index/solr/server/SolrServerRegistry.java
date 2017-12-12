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
package org.apache.jackrabbit.oak.plugins.index.solr.server;

import java.util.HashMap;
import java.util.Map;
import javax.annotation.CheckForNull;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServer;

/**
 * A registry for {@link org.apache.solr.client.solrj.SolrServer}s
 */
public class SolrServerRegistry {

    private static final Map<String, SolrClient> searchingServerRegistry = new HashMap<String, SolrClient>();
    private static final Map<String, SolrClient> indexingServerRegistry = new HashMap<String, SolrClient>();

    public static void register(@Nonnull SolrServerConfiguration configuration, @Nonnull SolrClient solrServer,
                                @Nonnull Strategy strategy) {
        switch (strategy) {
            case INDEXING:
                synchronized (indexingServerRegistry) {
                    indexingServerRegistry.put(configuration.toString(), solrServer);
                }
                break;
            case SEARCHING:
                synchronized (searchingServerRegistry) {
                    searchingServerRegistry.put(configuration.toString(), solrServer);
                }
                break;
        }
    }

    @CheckForNull
    public static SolrClient get(@Nonnull SolrServerConfiguration configuration, @Nonnull Strategy strategy) {
        switch (strategy) {
            case INDEXING:
                synchronized (indexingServerRegistry) {
                    return indexingServerRegistry.get(configuration.toString());
                }
            case SEARCHING:
                synchronized (searchingServerRegistry) {
                    return searchingServerRegistry.get(configuration.toString());
                }
        }
        return null;
    }

    public static void unregister(SolrServerConfiguration configuration, @Nonnull Strategy strategy) {
        switch (strategy) {
            case INDEXING:
                synchronized (indexingServerRegistry) {
                    SolrClient removed = indexingServerRegistry.remove(configuration.toString());
                    try {
                        removed.shutdown();
                    } catch (Exception e) {
                        // ignore
                    }
                }
                break;
            case SEARCHING:
                synchronized (searchingServerRegistry) {
                    SolrClient removed = searchingServerRegistry.remove(configuration.toString());
                    try {
                        removed.shutdown();
                    } catch (Exception e) {
                        // ignore
                    }
                }
                break;
        }
    }

    public enum Strategy {
        INDEXING,
        SEARCHING
    }

}
