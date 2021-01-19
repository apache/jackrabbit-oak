package org.apache.jackrabbit.oak.index;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.index.indexer.document.DocumentStoreIndexer;
import org.apache.jackrabbit.oak.index.indexer.document.ElasticIndexerProvider;
import org.apache.jackrabbit.oak.index.indexer.document.NodeStateIndexerProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;

import java.io.IOException;
import java.util.List;

public class ElasticDocumentStoreIndexer extends DocumentStoreIndexer {
    private final IndexHelper indexHelper;
    private final List<NodeStateIndexerProvider> indexerProviders;
    private final IndexerSupport indexerSupport;

    private final String indexPrefix;
    private final String scheme;
    private final String host;
    private final int port;
    private final String apiKeyId;
    private final String apiSecretId;

    public ElasticDocumentStoreIndexer(IndexHelper indexHelper, IndexerSupport indexerSupport,
                                       String indexPrefix, String scheme,
                                       String host, int port,
                                       String apiKeyId, String apiSecretId) throws IOException {
        super(indexHelper, indexerSupport);
        this.indexHelper = indexHelper;
        this.indexerSupport = indexerSupport;
        this.indexerProviders = createProviders();

        this.indexPrefix = indexPrefix;
        this.scheme = scheme;
        this.host = host;
        this.port = port;
        this.apiKeyId = apiKeyId;
        this.apiSecretId = apiSecretId;
    }

    protected List<NodeStateIndexerProvider> createProviders() {
        List<NodeStateIndexerProvider> providers = ImmutableList.of(
                createElasticIndexerProvider()
        );

        providers.forEach(closer::register);
        return providers;
    }

    private NodeStateIndexerProvider createElasticIndexerProvider() {
        final ElasticConnection.Builder.BuildStep buildStep = ElasticConnection.newBuilder()
                .withIndexPrefix(indexPrefix)
                .withConnectionParameters(
                        scheme,
                        host,
                        port
                );
        final ElasticConnection coordinate;
        if (apiKeyId != null && apiSecretId != null) {
            coordinate = buildStep.withApiKeys(apiKeyId, apiSecretId).build();
        } else {
            coordinate = buildStep.build();
        }
        closer.register(coordinate);
        return new ElasticIndexerProvider(indexHelper, coordinate);
    }

}
