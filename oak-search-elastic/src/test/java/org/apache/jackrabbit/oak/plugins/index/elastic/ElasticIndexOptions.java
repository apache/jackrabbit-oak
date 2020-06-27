package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.plugins.index.IndexOptions;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;

public class ElasticIndexOptions extends IndexOptions {

    @Override
    public String getIndexType() {
        return ElasticIndexDefinition.TYPE_ELASTICSEARCH;
    }

    protected IndexDefinitionBuilder createIndexDefinitionBuilder() {
        return new ElasticIndexDefinitionBuilder();
    }
}
