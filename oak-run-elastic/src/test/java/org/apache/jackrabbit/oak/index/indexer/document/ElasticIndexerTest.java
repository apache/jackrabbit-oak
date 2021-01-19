package org.apache.jackrabbit.oak.index.indexer.document;

import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticConnection;
import org.apache.jackrabbit.oak.plugins.index.elastic.ElasticIndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticIndexWriterFactory;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.progress.IndexingProgressReporter;
import org.apache.jackrabbit.oak.plugins.index.search.IndexDefinition;
import org.apache.jackrabbit.oak.plugins.index.search.spi.binary.FulltextBinaryTextExtractor;
import org.apache.jackrabbit.oak.plugins.index.search.spi.editor.FulltextIndexWriter;
import org.apache.jackrabbit.oak.spi.commit.CommitInfo;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import static org.apache.jackrabbit.oak.plugins.memory.EmptyNodeState.EMPTY_NODE;
import static org.apache.jackrabbit.oak.InitialContentHelper.INITIAL_CONTENT;

public class ElasticIndexerTest {

    private NodeState root = INITIAL_CONTENT;

    @Test
    public void nodeIndexed_WithIncludedPaths() throws Exception {
        ElasticIndexDefinitionBuilder idxb = new ElasticIndexDefinitionBuilder();
        idxb.indexRule("nt:base").property("foo").propertyIndex();
        idxb.includedPaths("/content");

        NodeState defn = idxb.build();
        IndexDefinition idxDefn = new ElasticIndexDefinition(root, defn, "/oak:index/testIndex", "testPrefix");

        NodeBuilder builder = root.builder();

        FulltextIndexWriter indexWriter = new ElasticIndexWriterFactory(mock(ElasticConnection.class)).newInstance(idxDefn, defn.builder(), CommitInfo.EMPTY, true);
        ElasticIndexer indexer = new ElasticIndexer(idxDefn, mock(FulltextBinaryTextExtractor.class), builder,
                mock(IndexingProgressReporter.class), indexWriter);

        NodeState testNode = EMPTY_NODE.builder().setProperty("foo", "bar").getNodeState();

        assertTrue(indexer.index(new NodeStateEntry(testNode, "/content/x")));
        assertFalse(indexer.index(new NodeStateEntry(testNode, "/x")));
        assertFalse(indexer.index(new NodeStateEntry(testNode, "/")));
    }

}
