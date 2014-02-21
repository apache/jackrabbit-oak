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
package org.apache.jackrabbit.oak.plugins.index.solr.util;

import java.io.IOException;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.CommitPolicy;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.DefaultSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.DefaultSolrServerProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.solr.client.solrj.SolrServer;
import org.apache.solr.client.solrj.SolrServerException;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleReference;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;

/**
 * Utilities for Oak Solr integration.
 */
public class OakSolrUtils {

    /**
     * adapts the OSGi Solr {@link org.apache.jackrabbit.oak.spi.query.QueryIndexProvider} service
     * 
     * @return a {@link org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider}
     */
    public static QueryIndexProvider adaptOsgiQueryIndexProvider() {
        QueryIndexProvider queryIndexProvider = null;
        try {
            BundleContext ctx = BundleReference.class
                            .cast(SolrQueryIndexProvider.class.getClassLoader()).getBundle()
                            .getBundleContext();

            ServiceReference serviceReference = ctx.getServiceReference(QueryIndexProvider.class
                            .getName());
            if (serviceReference != null) {
                queryIndexProvider = QueryIndexProvider.class
                                .cast(ctx.getService(serviceReference));
            }
        } catch (Throwable e) {
            // do nothing
        }
        return queryIndexProvider;
    }

    /**
     * adapt the OSGi Solr {@link SolrServerProvider} service of a given extending class and tries
     * to instantiate it if non existing.
     * 
     * @param providerClass
     *            the {@link Class} extending {@link SolrServerProvider} to adapt or instantiate
     * @param <T>
     *            the {@link SolrServerProvider} extension
     * @return a {@link SolrServerProvider} adapted from the OSGi service, or a directly
     *         instantiated one or <code>null</code> if both failed
     */
    public static <T extends SolrServerProvider> SolrServerProvider adaptOsgiSolrServerProvider(
                    Class<T> providerClass) {
        SolrServerProvider solrServerProvider = null;
        try {
            BundleContext ctx = FrameworkUtil.getBundle(providerClass).getBundleContext();
            ServiceReference serviceReference = ctx.getServiceReference(SolrServerProvider.class
                            .getName());
            if (serviceReference != null) {
                solrServerProvider = SolrServerProvider.class
                                .cast(ctx.getService(serviceReference));
            }
        } catch (Throwable e) {
            // do nothing
        }

        return solrServerProvider;
    }

    public static <T extends SolrServerProvider> SolrServerProvider getSolrServerProvider(
                    Class<T> providerClass) {
        SolrServerProvider solrServerProvider = adaptOsgiSolrServerProvider(providerClass);
        if (solrServerProvider == null && providerClass != null) {
            try {
                solrServerProvider = providerClass.newInstance();
            } catch (InstantiationException e) {
                // do nothing
            } catch (IllegalAccessException e) {
                // do nothing
            }
        }
        if (solrServerProvider == null) {
            solrServerProvider = new DefaultSolrServerProvider();
        }
        return solrServerProvider;
    }

    /**
     * adapt the OSGi Solr {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider} service of a given extending class
     * and tries to instantiate it if non existing.
     * 
     * @param providerClass
     *            the {@link Class} extending {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider} to adapt or
     *            instantiate
     * @param <T>
     *            the {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider} extension
     * @return a {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider} adapted from the OSGi service, or a directly
     *         instantiated one or <code>null</code> if both failed
     */
    public static <T extends OakSolrConfigurationProvider> OakSolrConfigurationProvider adaptOsgiOakSolrConfigurationProvider(
                    Class<T> providerClass) {
        OakSolrConfigurationProvider oakSolrConfigurationProvider = null;
        try {
            BundleContext ctx = FrameworkUtil.getBundle(providerClass).getBundleContext();
            ServiceReference serviceReference = ctx
                            .getServiceReference(OakSolrConfigurationProvider.class.getName());
            if (serviceReference != null) {
                oakSolrConfigurationProvider = OakSolrConfigurationProvider.class.cast(ctx
                                .getService(serviceReference));
            }
        } catch (Throwable e) {
            // do nothing
        }

        return oakSolrConfigurationProvider;
    }

    public static <T extends OakSolrConfigurationProvider> OakSolrConfigurationProvider getOakSolrConfigurationProvider(
                    Class<T> providerClass) {
        OakSolrConfigurationProvider oakSolrConfigurationProvider = OakSolrUtils
                        .adaptOsgiOakSolrConfigurationProvider(providerClass);
        if (oakSolrConfigurationProvider == null && providerClass != null) {
            try {
                oakSolrConfigurationProvider = providerClass.newInstance();
            } catch (InstantiationException e) {
                // do nothing
            } catch (IllegalAccessException e) {
                // do nothing
            }
        }
        if (oakSolrConfigurationProvider == null) {
            oakSolrConfigurationProvider = new DefaultSolrConfigurationProvider();
        }
        return oakSolrConfigurationProvider;
    }

    /**
     * Trigger a Solr commit on the basis of the given commit policy (e.g. hard, soft, auto)
     * 
     * @param solrServer
     *            the {@link org.apache.solr.client.solrj.SolrServer} used to communicate with the Solr instance
     * @param commitPolicy
     *            the {@link org.apache.jackrabbit.oak.plugins.index.solr.configuration.CommitPolicy} used to commit changes to a Solr index
     * @throws java.io.IOException
     *             if any low level IO error occurs
     * @throws org.apache.solr.client.solrj.SolrServerException
     *             if any error occurs while trying to communicate with the Solr instance
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
