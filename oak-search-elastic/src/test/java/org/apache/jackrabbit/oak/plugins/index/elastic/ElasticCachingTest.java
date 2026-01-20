package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ElasticCachingTest extends ElasticAbstractQueryTest {

    @Test
    public void testCaching() throws CommitFailedException {
        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        builder.includedPaths("/content");
        builder.indexRule("nt:base").property("a").propertyIndex();
        setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        Tree content = root.getTree("/").addChild("content");
        IntStream.range(0, 20).forEach(n -> {
                    Tree child = content.addChild("child_" + n);
                    child.setProperty("a", "text");
                }
        );
        root.commit(Map.of("sync-mode", "rt"));

        List<String> results = IntStream.range(0, 20).mapToObj(i -> "/content/child_" + i).collect(Collectors.toList());
        // First direct
        assertQuery("select [jcr:path] from [nt:base] where [a] = 'text'", results);

        // Second from cache
        assertQuery("select [jcr:path] from [nt:base] where [a] = 'text'", results);
    }

    @Test
    public void testCacheEviction() throws CommitFailedException {
        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        builder.includedPaths("/content");
        builder.indexRule("nt:base").property("a").propertyIndex();
        setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        Tree content = root.getTree("/").addChild("content");
        IntStream.range(0, 20).forEach(n -> {
                    Tree child = content.addChild("child_" + n);
                    child.setProperty("a", "text");
                }
        );
        root.commit(Map.of("sync-mode", "rt"));

        List<String> results = IntStream.range(0, 20).mapToObj(i -> "/content/child_" + i).collect(Collectors.toList());
        // First direct
        assertQuery("select [jcr:path] from [nt:base] where [a] = 'text'", results);

        Tree child = content.addChild("child_20");
        child.setProperty("a", "text");
        root.commit(Map.of("sync-mode", "rt"));

        List<String> resultsAfterModification = IntStream.range(0, 21).mapToObj(i -> "/content/child_" + i).collect(Collectors.toList());
        // Second from cache
        assertQuery("select [jcr:path] from [nt:base] where [a] = 'text'", resultsAfterModification);
    }

}
