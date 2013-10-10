package org.apache.jackrabbit.oak.plugins.index.solr.osgi;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nonnull;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.Reference;
import org.apache.felix.scr.annotations.ReferencePolicy;
import org.apache.felix.scr.annotations.ReferencePolicyOption;
import org.apache.felix.scr.annotations.Service;
import org.apache.jackrabbit.oak.plugins.index.solr.configuration.OakSolrConfigurationProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.solr.server.SolrServerProvider;
import org.apache.jackrabbit.oak.spi.query.QueryIndex;
import org.apache.jackrabbit.oak.spi.query.QueryIndexProvider;
import org.apache.jackrabbit.oak.spi.state.NodeState;

/**
 * Osgi Service that provides Solr based {@link org.apache.jackrabbit.oak.spi.query.QueryIndex}es
 * 
 * @see org.apache.jackrabbit.oak.plugins.index.solr.query.SolrQueryIndexProvider
 * @see QueryIndexProviderr
 */
@Component(metatype = false, immediate = true)
@Service(value = QueryIndexProvider.class)
public class SolrQueryIndexProviderService implements QueryIndexProvider {

    @Reference(policyOption = ReferencePolicyOption.GREEDY, policy = ReferencePolicy.STATIC)
    private SolrServerProvider solrServerProvider;

    @Reference(policyOption = ReferencePolicyOption.GREEDY, policy = ReferencePolicy.STATIC)
    private OakSolrConfigurationProvider oakSolrConfigurationProvider;

    private SolrQueryIndexProvider solrQueryIndexProvider;

    @Override
    @Nonnull
    public List<? extends QueryIndex> getQueryIndexes(NodeState nodeState) {
        List<? extends QueryIndex> queryIndexes = new ArrayList<QueryIndex>();
        if (solrServerProvider != null && oakSolrConfigurationProvider != null
                        && solrQueryIndexProvider == null) {
            solrQueryIndexProvider = new SolrQueryIndexProvider(solrServerProvider,
                            oakSolrConfigurationProvider);
            queryIndexes = solrQueryIndexProvider.getQueryIndexes(nodeState);
        }
        return queryIndexes;
    }

}
