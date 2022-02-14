package org.apache.jackrabbit.oak.index;

import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexImporter;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporter;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.spi.state.NodeStore;

import java.io.IOException;

public class ElasticIndexImporterSupport extends IndexImporterSupportBase{
    private final IndexHelper indexHelper;
    private final NodeStore nodeStore;

    public ElasticIndexImporterSupport(IndexHelper indexHelper) {
        super(indexHelper);
        this.indexHelper = indexHelper;
        this.nodeStore = indexHelper.getNodeStore();
    }

    @Override
    protected IndexEditorProvider createIndexEditorProvider() throws IOException {
        return null;
    }

    private void addImportProviders(IndexImporter importer) {
        importer.addImporterProvider(new ElasticIndexImporter(indexHelper.getGCBlobStore()));
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
