package org.apache.jackrabbit.oak.plugins.index.elastic.query;

import org.apache.jackrabbit.oak.plugins.index.search.spi.query.FulltextIndex.FulltextResultRow;

import java.util.Iterator;

public interface ElasticQueryIterator extends Iterator<FulltextResultRow> {
    String explain();
}
