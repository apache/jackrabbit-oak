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

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.nodetype.write.InitialContent;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.security.OpenSecurityProvider;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeState;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.createIndexDefinition;
import static org.apache.jackrabbit.oak.plugins.index.IndexUtils.getOrCreateOakIndex;

/**
 * Test for OAK-841
 */
public class EmptyPropertyTest extends AbstractQueryTest {

    @Override
    protected ContentRepository createRepository() {
        return new Oak()
                .with(new InitialContent())
                .with(new RepositoryInitializer() {
                    @Override
                    public NodeState initialize(NodeState state) {
                        NodeBuilder root = state.builder();
                        createIndexDefinition(getOrCreateOakIndex(root), "prop",
                                true, false, ImmutableList.of("prop"), null);
                        return root.getNodeState();
                    }
                }).with(new OpenSecurityProvider())
                .with(new PropertyIndexProvider())
                .with(new PropertyIndexEditorProvider())
                .createContentRepository();
    }

    @Ignore
    @Test
    public void emptyStringValue() throws CommitFailedException {
        Tree t = root.getTree("/");
        t.addChild("node-1").setProperty("prop", "value");
        root.commit();

        t = root.getTree("/");
        t.addChild("node-2").setProperty("prop", "");
        root.commit();
    }
}


