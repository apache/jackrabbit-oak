package org.apache.jackrabbit.oak.plugins.index.elasticsearch;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class ElasticsearchContentTest extends ElasticsearchAbstractQueryTest {

    @Test
    public void indexWithAnalyzedProperty() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        builder.indexRule("nt:base").property("a").analyzed();
        String testId = UUID.randomUUID().toString();
        Tree index = setIndex(testId, builder);
        root.commit();

        assertTrue(exists(index));
        assertThat(0L, equalTo(countDocuments(index)));

        Tree content = root.getTree("/").addChild(testId);
        content.addChild("indexed").setProperty("a", "foo");
        content.addChild("not-indexed").setProperty("b", "foo");
        root.commit();

        assertEventually(() -> assertThat(countDocuments(index), equalTo(1L)));

        content.getChild("indexed").remove();
        root.commit();

        assertEventually(() -> assertThat(countDocuments(index), equalTo(0L)));

        // TODO: should the index be deleted when the definition gets removed?
        //index.remove();
        //root.commit();

        //assertFalse(exists(index));
    }

    @Test
    @Ignore("this test fails because of a bug with nodeScopeIndex (every node gets indexed in an empty doc)")
    public void indexWithAnalyzedNodeScopeIndexProperty() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        builder.indexRule("nt:base").property("a").analyzed().nodeScopeIndex();
        String testId = UUID.randomUUID().toString();
        Tree index = setIndex(testId, builder);
        root.commit();

        assertThat(0L, equalTo(countDocuments(index)));

        Tree content = root.getTree("/").addChild(testId);
        content.addChild("indexed").setProperty("a", "foo");
        content.addChild("not-indexed").setProperty("b", "foo");
        root.commit();

        assertEventually(() -> assertThat(countDocuments(index), equalTo(1L)));
    }
}
