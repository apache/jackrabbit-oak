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
package org.apache.jackrabbit.oak.plugins.index.solr;

import java.io.IOException;
import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.index.SolrIndexHookProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleReference;
import org.osgi.framework.ServiceReference;

/**
 * Utilities for Oak Solr integration.
 */
public class OakSolrUtils {

    /**
     * Check if a given Solr instance is alive
     *
     * @param solrServer the {@link SolrServer} used to communicate with the Solr instance
     * @return <code>true</code> if the given Solr instance is alive and responding
     * @throws IOException         if any low level IO error occurs
     * @throws SolrServerException if any error occurs while trying to communicate with the Solr instance
     */
    public static boolean checkServerAlive(@Nonnull SolrServer solrServer) throws IOException, SolrServerException {
        return solrServer.ping().getStatus() == 0;
    }

    /**
     * adapts the OSGi Solr {@link IndexEditorProvider} service
     *
     * @return a {@link SolrIndexHookProvider}
     */
    public static IndexEditorProvider adaptOsgiIndexHookProvider() {
        IndexEditorProvider indexHookProvider = null;
        try {
            BundleContext ctx = BundleReference.class.cast(SolrIndexHookProvider.class
                    .getClassLoader()).getBundle().getBundleContext();

            ServiceReference serviceReference = ctx.getServiceReference(IndexEditorProvider.class.getName());
            if (serviceReference != null) {
                indexHookProvider = IndexEditorProvider.class.cast(ctx.getService(serviceReference));
            }
        } catch (Throwable e) {
            // do nothing
        }
        return indexHookProvider;
    }

    /**
     * adapts the OSGi Solr {@link QueryIndexProvider} service
     *
     * @return a {@link SolrQueryIndexProvider}
     */
    public static QueryIndexProvider adaptOsgiQueryIndexProvider() {
        QueryIndexProvider queryIndexProvider = null;
        try {
            BundleContext ctx = BundleReference.class.cast(SolrQueryIndexProvider.class
                    .getClassLoader()).getBundle().getBundleContext();

            ServiceReference serviceReference = ctx.getServiceReference(QueryIndexProvider.class.getName());
            if (serviceReference != null) {
                queryIndexProvider = QueryIndexProvider.class.cast(ctx.getService(serviceReference));
            }
        } catch (Throwable e) {
            // do nothing
        }
        return queryIndexProvider;
    }

    /**
     * adapt the OSGi Solr {@link SolrServerProvider} service of a given extending class
     * and tries to instantiate it if non existing.
     *
     * @param providerClass the {@link Class} extending {@link SolrServerProvider}
     *                      to adapt or instantiate
     * @param <T>           the {@link SolrServerProvider} extension
     * @return a {@link SolrServerProvider} adapted from the OSGi service, or a
     *         directly instantiated one or <code>null</code> if both failed
     */
    public static <T extends SolrServerProvider> SolrServerProvider adaptOsgiSolrServerProvider(Class<T> providerClass) {
        SolrServerProvider solrServerProvider = null;
        try {
            BundleContext ctx = BundleReference.class.cast(providerClass
                    .getClassLoader()).getBundle().getBundleContext();
            ServiceReference serviceReference = ctx.getServiceReference(SolrServerProvider.class.getName());
            if (serviceReference != null) {
                solrServerProvider = SolrServerProvider.class.cast(ctx.getService(serviceReference));
            }
        } catch (Exception e) {
            // do nothing
        }

        if (solrServerProvider == null && providerClass != null) {
            try {
                solrServerProvider = providerClass.newInstance();
            } catch (InstantiationException e) {
                // do nothing
            } catch (IllegalAccessException e) {
                // do nothing
            }
        }

        return solrServerProvider;
    }

    /**
     * adapt the OSGi Solr {@link OakSolrConfigurationProvider} service of a given
     * extending class and tries to instantiate it if non existing.
     *
     * @param providerClass the {@link Class} extending {@link OakSolrConfigurationProvider}
     *                      to adapt or instantiate
     * @param <T>           the {@link OakSolrConfigurationProvider} extension
     * @return a {@link OakSolrConfigurationProvider} adapted from the OSGi service, or a
     *         directly instantiated one or <code>null</code> if both failed
     */
    public static <T extends OakSolrConfigurationProvider> OakSolrConfigurationProvider adaptOsgiOakSolrConfigurationProvider(Class<T> providerClass) {
        OakSolrConfigurationProvider oakSolrConfigurationProvider = null;
        try {
            BundleContext ctx = BundleReference.class.cast(providerClass
                    .getClassLoader()).getBundle().getBundleContext();
            ServiceReference serviceReference = ctx.getServiceReference(OakSolrConfigurationProvider.class.getName());
            if (serviceReference != null) {
                oakSolrConfigurationProvider = OakSolrConfigurationProvider.class.cast(ctx.getService(serviceReference));
            }
        } catch (Exception e) {
            // do nothing
        }

        if (oakSolrConfigurationProvider == null && providerClass != null) {
            try {
                oakSolrConfigurationProvider = providerClass.newInstance();
            } catch (InstantiationException e) {
                // do nothing
            } catch (IllegalAccessException e) {
                // do nothing
            }
        }

        return oakSolrConfigurationProvider;
    }

    /**
     * Trigger a Solr commit on the basis of the given commit policy (e.g. hard, soft, auto)
     *
     * @param solrServer   the {@link SolrServer} used to communicate with the Solr instance
     * @param commitPolicy the {@link CommitPolicy} used to commit changes to a Solr index
     * @throws IOException         if any low level IO error occurs
     * @throws SolrServerException if any error occurs while trying to communicate with the Solr instance
     */
    public static void commitByPolicy(SolrServer solrServer, CommitPolicy commitPolicy)
            throws IOException, SolrServerException {
        switch (commitPolicy) {
            case HARD: {
                solrServer.commit();
                break;
            }
            case SOFT: {
                solrServer.commit(false, false, true);
                break;
            }
            case AUTO: {
                break;
            }
        }
    }
}
