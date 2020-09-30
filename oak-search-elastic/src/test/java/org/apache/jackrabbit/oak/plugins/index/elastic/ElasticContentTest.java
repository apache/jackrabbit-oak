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
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Random;
import java.util.UUID;

import static org.apache.jackrabbit.oak.plugins.index.elastic.ElasticTestUtils.randomString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;

public class ElasticContentTest extends ElasticAbstractQueryTest {

    @Test
    public void indexWithAnalyzedProperty() throws Exception {
        IndexDefinitionBuilder builder = createIndex("a").noAsync();
        builder.indexRule("nt:base").property("a").analyzed();
        Tree index = setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        assertTrue(exists(index));
        assertThat(0L, equalTo(countDocuments(index)));

        Tree content = root.getTree("/").addChild("content");
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
        index = setIndex(testId, builder);
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
}
