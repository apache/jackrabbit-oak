package org.apache.jackrabbit.oak.index;


import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static java.util.Arrays.asList;

public class ElasticOutOfBandIndexer extends OutOfBandIndexer {
    private final String indexPrefix;
    private final String scheme;
    private final String host;
    private final int port;
    private final String apiKeyId;
    private final String apiSecretId;

    public ElasticOutOfBandIndexer(IndexHelper indexHelper, IndexerSupport indexerSupport,
                                   String indexPrefix, String scheme,
                                   String host, int port,
                                   String apiKeyId, String apiSecretId) {
        super(indexHelper, indexerSupport);
        this.indexPrefix = indexPrefix;
        this.scheme = scheme;
        this.host = host;
        this.port = port;
        this.apiKeyId = apiKeyId;
        this.apiSecretId = apiSecretId;
    }

    @Override
    protected IndexEditorProvider createIndexEditorProvider() {
        IndexEditorProvider elastic = createElasticEditorProvider();

        return CompositeIndexEditorProvider.compose(asList(elastic));
    }

    private IndexEditorProvider createElasticEditorProvider() {
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
        ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(coordinate,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));

        return editorProvider;
    }
}
