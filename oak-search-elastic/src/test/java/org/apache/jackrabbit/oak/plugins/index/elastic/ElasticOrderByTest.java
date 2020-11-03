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
import org.junit.Test;

import javax.jcr.PropertyType;
import java.util.Collections;
import java.util.UUID;

import static java.util.Arrays.asList;

public class ElasticOrderByTest extends ElasticAbstractQueryTest {

    @Test
    public void withoutOrderByClause() throws Exception {
        IndexDefinitionBuilder builder = createIndex("foo");
        builder.indexRule("nt:base")
                .property("foo")
                .analyzed();

        setIndex(UUID.randomUUID().toString(), builder);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("foo", "hello");
        test.addChild("b").setProperty("foo", "hello hello");
        root.commit(Collections.singletonMap("sync-mode", "rt"));

        // results are sorted by score desc, node `b` returns first because it has a higher score from a tf/idf perspective
        assertOrderedQuery("select [jcr:path] from [nt:base] where contains(foo, 'hello')", asList("/test/b", "/test/a"));
    }

    @Test
    public void orderByScore() throws Exception {
        IndexDefinitionBuilder builder = createIndex("foo");
        builder.indexRule("nt:base")
                .property("foo")
                .analyzed();

        setIndex(UUID.randomUUID().toString(), builder);

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("foo", "hello");
        test.addChild("b").setProperty("foo", "hello hello");
        root.commit(Collections.singletonMap("sync-mode", "rt"));

        assertOrderedQuery("select [jcr:path] from [nt:base] where contains(foo, 'hello') order by [jcr:score]",
                asList("/test/a", "/test/b"));

        assertOrderedQuery("select [jcr:path] from [nt:base] where contains(foo, 'hello') order by [jcr:score] DESC",
                asList("/test/b", "/test/a"));
    }

    @Test
    public void orderByPath() throws Exception {
        IndexDefinitionBuilder builder = createIndex("foo");
        builder.indexRule("nt:base").property("foo");

        setIndex(UUID.randomUUID().toString(), builder);

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("foo", "aaaaaa");
        test.addChild("b").setProperty("foo", "bbbbbb");
        root.commit(Collections.singletonMap("sync-mode", "rt"));

        assertOrderedQuery("select [jcr:path] from [nt:base] order by [jcr:path]", asList("/test/a", "/test/b"));
        assertOrderedQuery("select [jcr:path] from [nt:base] order by [jcr:path] DESC", asList("/test/b", "/test/a"));
    }

    @Test
    public void orderByProperty() throws Exception {
        IndexDefinitionBuilder builder = createIndex("foo");
        builder.indexRule("nt:base").property("foo");

        setIndex(UUID.randomUUID().toString(), builder);

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("foo", "zzzzzz");
        test.addChild("b").setProperty("foo", "aaaaaa");
        root.commit(Collections.singletonMap("sync-mode", "rt"));

        assertOrderedQuery("select [jcr:path] from [nt:base] order by @foo", asList("/test/b", "/test/a"));
        assertOrderedQuery("select [jcr:path] from [nt:base] order by @foo DESC", asList("/test/a", "/test/b"));
    }

    @Test
    public void orderByAnalyzedProperty() throws Exception {
        IndexDefinitionBuilder builder = createIndex("foo");
        builder.indexRule("nt:base").property("foo").analyzed();

        setIndex(UUID.randomUUID().toString(), builder);

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("foo", "bbbbb");
        test.addChild("b").setProperty("foo", "hello aaaa");
        root.commit(Collections.singletonMap("sync-mode", "rt"));

        // this test verifies we use the keyword multi field when an analyzed properties is specified in order by
        // http://www.technocratsid.com/string-sorting-in-elasticsearch/

        assertOrderedQuery("select [jcr:path] from [nt:base] order by @foo", asList("/test/a", "/test/b"));
        assertOrderedQuery("select [jcr:path] from [nt:base] order by @foo DESC", asList("/test/b", "/test/a"));
    }

    @Test
    public void orderByNumericProperty() throws Exception {
        IndexDefinitionBuilder builder = createIndex("foo");
        builder.indexRule("nt:base").property("foo").type(PropertyType.TYPENAME_LONG);

        setIndex(UUID.randomUUID().toString(), builder);

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("foo", "10");
        test.addChild("b").setProperty("foo", "5");
        root.commit(Collections.singletonMap("sync-mode", "rt"));

        assertOrderedQuery("select [jcr:path] from [nt:base] order by @foo", asList("/test/b", "/test/a"));
        assertOrderedQuery("select [jcr:path] from [nt:base] order by @foo DESC", asList("/test/a", "/test/b"));
    }

    @Test
    public void orderByMultiProperties() throws Exception {
        IndexDefinitionBuilder builder = createIndex("foo", "bar");
        builder.indexRule("nt:base").property("foo");
        builder.indexRule("nt:base").property("bar").type(PropertyType.TYPENAME_LONG);

        setIndex(UUID.randomUUID().toString(), builder);

        Tree test = root.getTree("/").addChild("test");
        Tree a1 = test.addChild("a1");
        a1.setProperty("foo", "a");
        a1.setProperty("bar", "1");
        Tree a2 = test.addChild("a2");
        a2.setProperty("foo", "a");
        a2.setProperty("bar", "2");
        Tree b = test.addChild("b");
        b.setProperty("foo", "b");
        b.setProperty("bar", "100");
        root.commit(Collections.singletonMap("sync-mode", "rt"));

        assertOrderedQuery("select [jcr:path] from [nt:base] order by @foo, @bar",
                asList("/test/a1", "/test/a2", "/test/b"));

        assertOrderedQuery("select [jcr:path] from [nt:base] order by @foo, @bar DESC",
                asList("/test/a2", "/test/a1", "/test/b"));

        assertOrderedQuery("select [jcr:path] from [nt:base] order by @bar DESC, @foo DESC",
                asList("/test/b", "/test/a2", "/test/a1"));
    }
}
