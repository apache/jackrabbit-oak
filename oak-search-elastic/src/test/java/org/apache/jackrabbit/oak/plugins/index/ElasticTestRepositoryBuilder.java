package org.apache.jackrabbit.oak.plugins.index;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.plugins.index.counter.NodeCounterEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.query.ElasticIndexProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider.compose;

public class ElasticTestRepositoryBuilder extends TestRepositoryBuilder {

    private ElasticConnection esConnection;

    public ElasticTestRepositoryBuilder(ElasticConnectionRule elasticRule) {
        this.esConnection = elasticRule.useDocker() ? elasticRule.getElasticConnectionForDocker() :
                elasticRule.getElasticConnectionFromString();
        this.editorProvider = (ElasticIndexEditorProvider) getIndexEditorProvider();
        this.indexProvider = new ElasticIndexProvider(esConnection);
        this.asyncIndexUpdate = new AsyncIndexUpdate("async", nodeStore, compose(newArrayList(
                editorProvider,
                new NodeCounterEditorProvider()
        )));
        asyncIndexUpdate.setCorruptIndexHandler(trackingCorruptIndexHandler);
    }

    public RepositoryOptionsUtil build() {
        Oak oak = new Oak(nodeStore)
                .with(initialContent)
                .with(securityProvider)
                .with(editorProvider)
                .with(indexProvider)
                .with(indexProvider)
                .with(queryIndexProvider);
        if (isAsync) {
            oak.withAsyncIndexing("async", defaultAsyncIndexingTimeInSeconds);
        }
        return new RepositoryOptionsUtil(oak).with(isAsync).with(asyncIndexUpdate);
    }

    private IndexEditorProvider getIndexEditorProvider() {
        return new ElasticIndexEditorProvider(esConnection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
    }
}
