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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import javax.jcr.query.Query;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
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
                    public void initialize(NodeBuilder builder) {
                        NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);
                        IndexUtils.createIndexDefinition(
                                index, "xyz", true, false,
                                ImmutableList.<String>of("x", "y", "z"), null);
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
        setTravesalEnabled(false);
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
        assertTrue(lines.get(0), lines.get(0).contains("property xyz IN (foo, bar)"));

        assertQuery(
                "select [jcr:path] from [nt:base] where [x] = 'foo' OR [y] = 'foo'",
                ImmutableList.of("/a"));
        assertQuery(
                "select [jcr:path] from [nt:base] where [x] = 'foo' OR [z] = 'foo'",
                ImmutableList.of("/a", "/c"));
        assertQuery(
                "select [jcr:path] from [nt:base] where [x] = 'foo' OR [y] = 'bar'",
                ImmutableList.of("/a", "/b"));
        setTravesalEnabled(false);
    }
}
