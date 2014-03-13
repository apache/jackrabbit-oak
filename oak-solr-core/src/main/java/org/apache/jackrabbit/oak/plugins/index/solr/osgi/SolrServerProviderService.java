package org.apache.jackrabbit.oak.plugins.index.solr.osgi;

import java.util.HashMap;
import java.util.Map;
import org.apache.felix.scr.annotations.Activate;
import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Deactivate;
import org.apache.felix.scr.annotations.Property;
import org.apache.felix.scr.annotations.PropertyOption;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferenceCardinality;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.References;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfiguration;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.SolrServerConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.solr.client.solrj.SolrServer;
import org.osgi.service.component.ComponentContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * OSGi service for {@link org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider}
 */
@Component(metatype = true, label = "Oak Solr server provider", immediate = true)
@References({
        @Reference(name = "solrServerConfigurationProvider",
                referenceInterface = SolrServerConfigurationProvider.class,
                cardinality = ReferenceCardinality.MANDATORY_MULTIPLE,
                policy = ReferencePolicy.DYNAMIC,
                bind = "bindSolrServerConfigurationProvider",
                unbind = "unbindSolrServerConfigurationProvider",
                updated = "updateSolrServerConfigurationProvider"
        )
})
@Service(SolrServerProvider.class)
public class SolrServerProviderService implements SolrServerProvider {

    private final Logger log = LoggerFactory.getLogger(getClass());

    @Property(options = {
            @PropertyOption(name = "none",
                    value = "None"
            ),
            @PropertyOption(name = "embedded",
                    value = "Embedded Solr"
            ),
            @PropertyOption(name = "remote",
                    value = "Remote Solr"
            )},
            value = "none"
    )
    private static final String SERVER_TYPE = "server.type";

    private final Map<String, SolrServerConfigurationProvider> solrServerConfigurationProviders = new HashMap<String, SolrServerConfigurationProvider>();

    private String serverType;

    private SolrServer cachedSolrServer;

    private SolrServerConfiguration cachedSolrServerConfiguration;

    @Activate
    protected void activate(ComponentContext context) throws Exception {
        serverType = String.valueOf(context.getProperties().get(SERVER_TYPE));
    }

    @Deactivate
    protected void deactivate() throws Exception {
        solrServerConfigurationProviders.clear();
        if (cachedSolrServer != null) {
            try {
                cachedSolrServer.shutdown();
            } catch (Exception e) {
                log.error("could not correctly shutdown Solr {} server {}", serverType, cachedSolrServer);
            } finally {
                cachedSolrServer = null;
            }
        }
    }

    protected void bindSolrServerConfigurationProvider(final SolrServerConfigurationProvider solrServerConfigurationProvider, Map<String, Object> properties) {
        synchronized (solrServerConfigurationProviders) {
            String name = String.valueOf(properties.get("name"));
            solrServerConfigurationProviders.put(name, solrServerConfigurationProvider);
        }
    }

    protected void unbindSolrServerConfigurationProvider(final SolrServerConfigurationProvider solrServerConfigurationProvider, Map<String, Object> properties) {
        synchronized (solrServerConfigurationProviders) {
            String name = String.valueOf(properties.get("name"));
            solrServerConfigurationProviders.remove(name);
        }
    }

    protected void updatedSolrServerConfigurationProvider(final SolrServerConfigurationProvider solrServerConfigurationProvider, Map<String, Object> properties) {
        synchronized (solrServerConfigurationProviders) {
            String name = String.valueOf(properties.get("name"));
            solrServerConfigurationProviders.put(name, solrServerConfigurationProvider);
        }
    }

    @Override
    public SolrServer getSolrServer() throws Exception {
        SolrServer solrServer = null;
        synchronized (solrServerConfigurationProviders) {
            if (serverType != null && !"none".equals(serverType)) {
                SolrServerConfigurationProvider solrServerConfigurationProvider = solrServerConfigurationProviders.get(serverType);
                try {
                    if (solrServerConfigurationProvider != null) {
                        SolrServerConfiguration solrServerConfiguration = solrServerConfigurationProvider.getSolrServerConfiguration();
                        if (solrServerConfiguration != cachedSolrServerConfiguration) {
                            SolrServerProvider solrServerProvider = solrServerConfiguration.newInstance();
                            cachedSolrServerConfiguration = solrServerConfiguration;
                            if (cachedSolrServer != null) {
                                cachedSolrServer.shutdown();
                            }
                            cachedSolrServer = solrServerProvider.getSolrServer();
                        } else {
                            solrServer = cachedSolrServer;
                        }
                    }
                } catch (Exception e) {
                    log.error("could not get a SolrServerProvider of type {}, {}", serverType, e);
                }
            }
        }
        return solrServer;
    }

}
