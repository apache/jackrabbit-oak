package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;

public class LuceneIndexOptions extends IndexOptions {

    @Override
    public String getIndexType() {
        return "lucene";
    }

    protected IndexDefinitionBuilder createIndexDefinitionBuilder() {
        return new LuceneIndexDefinitionBuilder();
    }

}
