package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporterProvider;
import org.apache.jackrabbit.oak.plugins.index.search.ReindexOperations;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;

import java.io.File;
import java.io.IOException;

public class ElasticIndexImporter implements IndexImporterProvider {

    public ElasticIndexImporter(){

    }

    @Override
    public void importIndex(NodeState root, NodeBuilder definitionBuilder, File indexDir) throws IOException, CommitFailedException {

        ReindexOperations reindexOps = new ReindexOperations(root, definitionBuilder, "",
                new ElasticIndexDefinition.Builder());

    }

    @Override
    public String getType() {
        return ElasticIndexDefinition.TYPE_ELASTICSEARCH;
    }
}
