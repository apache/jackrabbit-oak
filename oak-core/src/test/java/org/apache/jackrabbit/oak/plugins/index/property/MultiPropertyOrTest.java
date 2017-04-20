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
package org.apache.jackrabbit.oak.plugins.index.property;

import static java.util.Arrays.asList;
import static org.apache.jackrabbit.oak.api.QueryEngine.NO_MAPPINGS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.matchers.JUnitMatchers.containsString;

import java.text.ParseException;
import java.util.List;
import java.util.Random;

import javax.annotation.Nonnull;
import javax.jcr.query.Query;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.PropertyValue;
import org.apache.jackrabbit.oak.api.ResultRow;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

/**
 * <code>RelativePathTest</code>...
 */
public class MultiPropertyOrTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {
        return new Oak().with(new InitialContent())
                .with(new RepositoryInitializer() {
                    @Override
                    public void initialize(@Nonnull NodeBuilder builder) {
                        NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);
                        IndexUtils.createIndexDefinition(
                                index, "xyz", true, false,
                                ImmutableList.of("x", "y", "z", "w"), null);
                    }
                })
                .with(new OpenSecurityProvider())
                .with(new PropertyIndexProvider())
                .with(new PropertyIndexEditorProvider())
                .createContentRepository();
    }

    @Test
    public void query() throws Exception {
        Tree t = root.getTree("/");
        t.addChild("a").setProperty("x", "foo");
        t.addChild("b").setProperty("y", "bar");
        t.addChild("c").setProperty("z", "foo");
        root.commit();
        setTraversalEnabled(false);
        assertQuery("select [jcr:path] from [nt:base] where [x] is not null",
                ImmutableList.of("/a"));

        List<String> lines = executeQuery(
                "explain select [jcr:path] from [nt:base] where [x] is not null",
                Query.JCR_SQL2);
        assertEquals(1, lines.size());
        // make sure it used the property index
        assertTrue(lines.get(0).contains("property xyz IS NOT NULL"));

        lines = executeQuery(
                "explain select [jcr:path] from [nt:base] where [x] = 'foo' OR [y] = 'foo'",
                Query.JCR_SQL2);
        assertEquals(1, lines.size());
        // make sure it used the property index
        assertTrue(lines.get(0).contains("property xyz = foo"));

        lines = executeQuery(
                "explain select [jcr:path] from [nt:base] where [x] = 'foo' OR [y] = 'bar'",
                Query.JCR_SQL2);
        assertEquals(1, lines.size());
        // make sure it used the property index
        assertTrue(lines.get(0), lines.get(0).contains("property xyz = foo"));
        assertTrue(lines.get(0), lines.get(0).contains("property xyz = bar"));

        assertQuery(
                "select [jcr:path] from [nt:base] where [x] = 'foo' OR [y] = 'foo'",
                ImmutableList.of("/a"));
        assertQuery(
                "select [jcr:path] from [nt:base] where [x] = 'foo' OR [z] = 'foo'",
                ImmutableList.of("/a", "/c"));
        assertQuery(
                "select [jcr:path] from [nt:base] where [x] = 'foo' OR [y] = 'bar'",
                ImmutableList.of("/a", "/b"));
        setTraversalEnabled(false);
    }

    @Test
    public void unionSortResultCount() throws Exception {
        // create test data
        Tree test = root.getTree("/").addChild("test");
        root.commit();

        List<Integer> nodes = Lists.newArrayList();
        Random r = new Random();
        int seed = -2;
        for (int i = 0; i < 1000; i++) {
            Tree a = test.addChild("a" + i);
            a.setProperty("x", "fooa");
            seed += 2;
            int num = r.nextInt(100);
            a.setProperty("z", num);
            nodes.add(num);
        }

        seed = -1;
        for (int i = 0; i < 1000; i++) {
            Tree a = test.addChild("b" + i);
            a.setProperty("y", "foob");
            seed += 2;
            int num = 100 + r.nextInt(100);
            a.setProperty("z",  num);
            nodes.add(num);
        }
        root.commit();

        // scan count scans the whole result set
        String query =
            "measure /jcr:root//element(*, nt:base)[(@x = 'fooa' or @y = 'foob')] order by @z";
        assertThat(measureWithLimit(query, XPATH, 100), containsString("scanCount: 2000"));
    }

    @Test
    public void unionSortQueries() throws Exception {
        // create test data
        Tree test = root.getTree("/").addChild("test");
        root.commit();

        int seed = -3;
        for (int i = 0; i < 5; i++) {
            Tree a = test.addChild("a" + i);
            a.setProperty("x", "a" + i);
            seed += 3;
            a.setProperty("w", seed);
        }

        seed = -2;
        for (int i = 0; i < 5; i++) {
            Tree a = test.addChild("b" + i);
            a.setProperty("y", "b" + i);
            seed += 3;
            a.setProperty("w", seed);
        }
        seed = -1;
        for (int i = 0; i < 5; i++) {
            Tree a = test.addChild("c" + i);
            a.setProperty("z", "c" + i);
            seed += 3;
            a.setProperty("w", seed);
        }
        root.commit();

        assertQuery(
            "/jcr:root//element(*, nt:base)[(@x = 'a4' or @y = 'b3')] order by @w",
            XPATH,
            asList("/test/b3", "/test/a4"));
        assertQuery(
            "/jcr:root//element(*, nt:base)[(@x = 'a3' or @y = 'b0' or @z = 'c2')] order by @w",
            XPATH,
            asList("/test/b0", "/test/c2", "/test/a3"));
    }

    private String measureWithLimit(String query, String lang, int limit) throws ParseException {
        List<? extends ResultRow> result = Lists.newArrayList(
            qe.executeQuery(query, lang, limit, 0, Maps.<String, PropertyValue>newHashMap(),
                NO_MAPPINGS).getRows());

        String measure = "";
        if (result.size() > 0) {
            measure = result.get(0).toString();
        }
        return measure;
    }
}
