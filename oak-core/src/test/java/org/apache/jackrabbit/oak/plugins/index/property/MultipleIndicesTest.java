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

import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getOrCreateOakIndex;

import java.util.ArrayList;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class MultipleIndicesTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {
        return new Oak()
                .with(new InitialContent())
                .with(new RepositoryInitializer() {
                    @Override
                    public void initialize(@Nonnull NodeBuilder builder) {
                        createIndexDefinition(
                                getOrCreateOakIndex(builder), "pid",
                                true, false, ImmutableList.of("pid"), null);
                        createIndexDefinition(
                                getOrCreateOakIndex(builder.child("content")),
                                "pid", true, false, ImmutableList.of("pid"),
                                null);
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
        t.setProperty("pid", "foo");
        t.addChild("a").setProperty("pid", "foo");
        t.addChild("b").setProperty("pid", "bar");
        t.addChild("c").setProperty("pid", "foo");
        t.addChild("d").setProperty("cid", "foo");

        Tree content = t.addChild("content");
        content.addChild("x").setProperty("pid", "foo");
        content.addChild("y").setProperty("pid", "baz");
        content.addChild("z").setProperty("pid", "bar");
        root.commit();

        setTraversalEnabled(false);
        assertQuery("select [jcr:path] from [nt:base] where [cid] = 'foo'",
                new ArrayList<String>());

        assertQuery("select [jcr:path] from [nt:base] where [pid] = 'foo'",
                ImmutableList.of("/", "/a", "/c", "/content/x"));

        assertQuery("select [jcr:path] from [nt:base] where [pid] = 'bar'",
                ImmutableList.of("/b", "/content/z"));

        assertQuery("select [jcr:path] from [nt:base] where [pid] = 'baz'",
                ImmutableList.of("/content/y"));
        setTraversalEnabled(true);
    }

    /**
     * Test for OAK-841
     */
    @Test
    public void emptyStringValue() throws CommitFailedException {
        Tree t = root.getTree("/");
        t.addChild("node-1").setProperty("pid", "value");
        root.commit();

        t = root.getTree("/");
        t.addChild("node-2").setProperty("pid", "");
        root.commit();

        t = root.getTree("/");
        t.addChild("node-3").setProperty("pid", ":");
        root.commit();

        setTraversalEnabled(false);
        assertQuery("select [jcr:path] from [nt:base] where [pid] = 'value'",
                ImmutableList.of("/node-1"));
        assertQuery("select [jcr:path] from [nt:base] where [pid] = ''",
                ImmutableList.of("/node-2"));
        assertQuery("select [jcr:path] from [nt:base] where [pid] = ':'",
                ImmutableList.of("/node-3"));

        setTraversalEnabled(true);
    }
}
