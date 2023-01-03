/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.elastic;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticTestUtils.randomString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.AdditionalMatchers.geq;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ElasticContentTest extends ElasticAbstractQueryTest {

    private final ElasticMetricHandler spyMetricHandler =
            spy(new ElasticMetricHandler(StatisticsProvider.NOOP));

    @Override
    protected ElasticMetricHandler getMetricHandler() {
        return spyMetricHandler;
    }

    @Test
    public void indexWithAnalyzedProperty() throws Exception {
        reset(spyMetricHandler);
        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        builder.indexRule("nt:base").property("a").analyzed();
        String indexName = UUID.randomUUID().toString();
        String indexNameWithPrefix = esConnection.getIndexPrefix() + "." + indexName;
        Tree index = setIndex(indexName, builder);
        root.commit();

        assertTrue(exists(index));
        assertThat(0L, equalTo(countDocuments(index)));
        // there are no updates, so metrics won't be refreshed
        verify(spyMetricHandler, never()).markSize(anyString(), anyLong(), anyLong());
        verify(spyMetricHandler, never()).markDocuments(anyString(), anyLong());

        reset(spyMetricHandler);
        Tree content = root.getTree("/").addChild("content");
        content.addChild("indexed").setProperty("a", "foo");
        content.addChild("not-indexed").setProperty("b", "foo");
        root.commit();

        verify(spyMetricHandler).markSize(eq(indexNameWithPrefix), geq(0L), geq(0L));
        verify(spyMetricHandler).markDocuments(eq(indexNameWithPrefix), geq(0L));
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
        Tree index = setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        assertThat(0L, equalTo(countDocuments(index)));

        Tree content = root.getTree("/").addChild("content");
        content.addChild("indexed").setProperty("a", "foo");
        content.addChild("not-indexed").setProperty("b", "foo");
        root.commit();

        assertEventually(() -> assertThat(countDocuments(index), equalTo(1L)));
    }

    @Test
    public void indexContentWithLongPath() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        builder.indexRule("nt:base").property("a").analyzed();
        Tree index = setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        assertTrue(exists(index));
        assertThat(0L, equalTo(countDocuments(index)));

        int leftLimit = 48; // ' ' (space)
        int rightLimit = 122; // char '~'
        int targetStringLength = 1024;
        final Random random = new Random(42);

        String generatedPath = random.ints(leftLimit, rightLimit + 1)
                .limit(targetStringLength)
                .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                .toString();

        Tree content = root.getTree("/").addChild("content");
        content.addChild(generatedPath).setProperty("a", "foo");
        root.commit();

        assertEventually(() -> assertThat(countDocuments(index), equalTo(1L)));
    }

    @Test
    public void defineIndexTwice() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        String testId = UUID.randomUUID().toString();
        Tree index = setIndex(testId, builder);
        root.commit();

        assertTrue(exists(index));

        builder = createIndex("a").noAsync();
        setIndex(testId, builder);
        root.commit();
    }

    @Test
    public void analyzedFieldWithLongValue() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        builder.indexRule("nt:base").property("a").analyzed();
        Tree index = setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        assertTrue(exists(index));
        assertThat(0L, equalTo(countDocuments(index)));

        Tree content = root.getTree("/").addChild("content");
        content.addChild("indexed1").setProperty("a", randomString(33409)); // max length is 32766
        root.commit();

        assertEventually(() -> assertThat(countDocuments(index), equalTo(1L)));
    }

    @Test
    public void indexWithCustomFetchSizes() throws Exception {
        BiConsumer<String, Iterable<Long>> buildIndex = (p, fetchSizes) -> {
            IndexDefinitionBuilder builder = createIndex(p).noAsync();
            builder.getBuilderTree().setProperty("queryFetchSizes", fetchSizes, Type.LONGS);
            builder.indexRule("nt:base").property(p).propertyIndex();
            setIndex(p + "_" + UUID.randomUUID(), builder);
        };

        buildIndex.accept("a", Collections.singletonList(1L));
        buildIndex.accept("b", Arrays.asList(1L, 2L));
        buildIndex.accept("c", Arrays.asList(3L, 100L));
        root.commit();

        Tree content = root.getTree("/").addChild("content");
        IntStream.range(0, 3).forEach(n -> {
                    Tree child = content.addChild("child_" + n);
                    child.setProperty("a", "text");
                    child.setProperty("b", "text");
                    child.setProperty("c", "text");
                }
        );
        root.commit(Collections.singletonMap("sync-mode", "rt"));

        List<String> results = Arrays.asList("/content/child_0", "/content/child_1", "/content/child_2");

        reset(spyMetricHandler);
        assertQuery("select [jcr:path] from [nt:base] where [a] = 'text'", results);
        verify(spyMetricHandler, times(3)).markQuery(anyString(), anyBoolean());

        reset(spyMetricHandler);
        assertQuery("select [jcr:path] from [nt:base] where [b] = 'text'", results);
        verify(spyMetricHandler, times(2)).markQuery(anyString(), anyBoolean());

        reset(spyMetricHandler);
        assertQuery("select [jcr:path] from [nt:base] where [c] = 'text'", results);
        verify(spyMetricHandler, times(1)).markQuery(anyString(), anyBoolean());
    }

    @Test
    public void indexWithLowTrackTotalHits() throws Exception {
        BiConsumer<String, Iterable<Long>> buildIndex = (p, fetchSizes) -> {
            IndexDefinitionBuilder builder = createIndex(p).noAsync();
            builder.getBuilderTree().setProperty("queryFetchSizes", fetchSizes, Type.LONGS);
            builder.getBuilderTree().setProperty("trackTotalHits", 10L, Type.LONG);
            builder.indexRule("nt:base").property(p).propertyIndex();
            setIndex(p + "_" + UUID.randomUUID(), builder);
        };

        buildIndex.accept("a", Collections.singletonList(10L));
        root.commit();

        Tree content = root.getTree("/").addChild("content");

        List<String> results = IntStream.range(0, 100)
                .mapToObj(n -> {
                    Tree child = content.addChild("child_" + n);
                    child.setProperty("a", "text");
                    return "/content/child_" + n;
                })
                .collect(Collectors.toList());

        root.commit();

        assertEventually(() -> assertQuery("select [jcr:path] from [nt:base] where [a] = 'text'", results));
    }
}
