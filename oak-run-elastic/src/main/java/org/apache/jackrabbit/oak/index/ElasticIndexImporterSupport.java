package org.apache.jackrabbit.oak.index;

import com.google.common.io.Closer;
import org.apache.commons.io.FileUtils;
import org.apache.jackrabbit.oak.plugins.index.CompositeIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.IndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexImporter;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexTracker;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticMetricHandler;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporter;
import org.apache.jackrabbit.oak.plugins.index.property.PropertyIndexEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.reference.ReferenceEditorProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ExtractedTextCache;
import org.apache.jackrabbit.oak.spi.mount.MountInfoProvider;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;

public class ElasticIndexImporterSupport extends IndexImporterSupportBase implements Closeable {
    private final String indexPrefix;
    private final String scheme;
    private final String host;
    private final int port;
    private final String apiKeyId;
    private final String apiSecretId;
    protected final Closer closer = Closer.create();

    public ElasticIndexImporterSupport(IndexHelper indexHelper ,String indexPrefix, String scheme,
                                       String host, int port,
                                       String apiKeyId, String apiSecretId) {
        super(indexHelper);
        this.indexPrefix = indexPrefix;
        this.scheme = scheme;
        this.host = host;
        this.port = port;
        this.apiKeyId = apiKeyId;
        this.apiSecretId = apiSecretId;
    }

    @Override
    protected IndexEditorProvider createIndexEditorProvider() {
        /*IndexEditorProvider elastic = createElasticEditorProvider();
        return CompositeIndexEditorProvider.compose(Collections.singletonList(elastic));*/
        MountInfoProvider mip = indexHelper.getMountInfoProvider();
        //Later we can add support for property index and other indexes here
        return new CompositeIndexEditorProvider(
                createElasticEditorProvider(),
                new PropertyIndexEditorProvider().with(mip),
                new ReferenceEditorProvider().with(mip)
        );
    }

    @Override
    protected void addImportProviders(IndexImporter importer) {
        importer.addImporterProvider(new ElasticIndexImporter());
    }

    private IndexEditorProvider createElasticEditorProvider() {
        final ElasticConnection.Builder.BuildStep buildStep = ElasticConnection.newBuilder()
                .withIndexPrefix(indexPrefix)
                .withConnectionParameters(
                        scheme,
                        host,
                        port
                );
        final ElasticConnection connection;
        if (apiKeyId != null && apiSecretId != null) {
            connection = buildStep.withApiKeys(apiKeyId, apiSecretId).build();
        } else {
            connection = buildStep.build();
        }
        closer.register(connection);
        ElasticIndexTracker indexTracker = new ElasticIndexTracker(connection,
                new ElasticMetricHandler(StatisticsProvider.NOOP));
        ElasticIndexEditorProvider editorProvider = new ElasticIndexEditorProvider(indexTracker, connection,
                new ExtractedTextCache(10 * FileUtils.ONE_MB, 100));
        return editorProvider;
    }

    @Override
    public void close() throws IOException {
        closer.close();
    }
}
