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

import javax.annotation.Nonnull;
import javax.jcr.query.Query;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.ContentRepository;
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
public class RelativePathTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {
        return new Oak().with(new InitialContent())
                .with(new RepositoryInitializer() {
                    @Override
                    public void initialize(@Nonnull NodeBuilder builder) {
                        NodeBuilder index = IndexUtils.getOrCreateOakIndex(builder);
                        IndexUtils.createIndexDefinition(index, "myProp", true,
                                false, ImmutableList.<String>of("myProp"), null);
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
        t.addChild("a").addChild("n").setProperty("myProp", "foo");
        t.addChild("b").addChild("n").setProperty("myProp", "bar");
        t.addChild("c").addChild("x").setProperty("myProp", "foo");
        t.setProperty("myProp", "foo");
        root.commit();
        setTraversalEnabled(false);
        assertQuery("select [jcr:path] from [nt:base] where [n/myProp] is not null",
                ImmutableList.of("/a", "/b"));

        List<String> lines = executeQuery(
                "explain select [jcr:path] from [nt:base] where [n/myProp] is not null",
                Query.JCR_SQL2);
        assertEquals(1, lines.size());
        // make sure it used the property index
        assertTrue(lines.get(0).contains("property myProp"));

        assertQuery(
                "select [jcr:path] from [nt:base] where [n/myProp] = 'foo'",
                ImmutableList.of("/a"));
        setTraversalEnabled(false);
    }
}
