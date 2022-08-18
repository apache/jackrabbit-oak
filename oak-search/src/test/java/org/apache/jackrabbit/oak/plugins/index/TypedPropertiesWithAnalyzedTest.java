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
package org.apache.jackrabbit.oak.plugins.index;

import com.google.common.collect.ImmutableList;
import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Test;

import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertFalse;

public abstract class TypedPropertiesWithAnalyzedTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtils.assertEventually(r, ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(true);
    }

    protected Tree setIndex(IndexDefinitionBuilder builder, String idxName) {
        return builder.build(root.getTree("/").addChild(IndexConstants.INDEX_DEFINITIONS_NAME).addChild(idxName));
    }


    @Test
    public void typeLongAnalyzed() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.noAsync();
        builder.indexRule("nt:base")
                .property("propa")
                .analyzed()
                .type("Long");
        setIndex(builder, UUID.randomUUID().toString());
        root.commit();

        addContent();
        runQueries();
    }

    @Test
    public void typeDateAnalyzed() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.noAsync();
        builder.indexRule("nt:base")
                .property("propa")
                .analyzed()
                .type("Date");
        setIndex(builder, UUID.randomUUID().toString());
        root.commit();

        addContent();
        runQueries();
    }

    @Test
    public void typeBinaryAnalyzed() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.noAsync();
        builder.indexRule("nt:base")
                .property("propa")
                .analyzed()
                .type("Binary");
        setIndex(builder, UUID.randomUUID().toString());
        root.commit();

        addContent();
        runQueries();
    }

    @Test
    public void typeDoubleAnalyzed() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.noAsync();
        builder.indexRule("nt:base")
                .property("propa")
                .analyzed()
                .type("Double");
        setIndex(builder, UUID.randomUUID().toString());
        root.commit();

        addContent();
        runQueries();
    }

    protected void prepareIndexForBooleanPropertyTest() throws CommitFailedException {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.noAsync();
        builder.indexRule("nt:base")
                .property("propa")
                .analyzed()
                .type("Boolean");
        setIndex(builder, UUID.randomUUID().toString());
        root.commit();
        addContent();
    }

    @Test
    public void typeStringAnalyzed() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.noAsync();
        builder.indexRule("nt:base")
                .property("propa")
                .analyzed()
                .type("String");
        setIndex(builder, UUID.randomUUID().toString());
        root.commit();

        addContent();
        runQueries();
    }

    private void addContent() throws CommitFailedException {
        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", 1234);
        test.addChild("b").setProperty("propa", "1234");
        test.addChild("c").setProperty("propa", "1234abcd");
        test.addChild("d").setProperty("propa", "1234.12");
        test.addChild("e").setProperty("propa", "true");
        test.addChild("f").setProperty("propa", 4321);
        test.addChild("g").setProperty("propa", "4321");
        test.addChild("h").setProperty("propa", "4321abcd");
        test.addChild("i").setProperty("propa", "4321.12");
        test.addChild("j").setProperty("propa", "false");
        test.addChild("k").setProperty("propa", "LoremIpsum");
        root.commit();
    }

    private void runQueries() {
        assertEventually(() -> {
            List<String> result = executeQuery("/jcr:root//*[jcr:contains(@propa, '123*')]", XPATH, true);
            assertFalse(result.isEmpty());
        });

        assertQuery("/jcr:root//*[jcr:contains(@propa, '123*')]", XPATH,
                ImmutableList.of("/test/a", "/test/b", "/test/c", "/test/d"));
        assertQuery("/jcr:root//*[jcr:contains(@propa, '432*')]", XPATH,
                ImmutableList.of("/test/f", "/test/g", "/test/h", "/test/i"));
        assertQuery("/jcr:root//*[jcr:contains(@propa, 'notpresent*')]", XPATH,
                ImmutableList.of());
        assertQuery("/jcr:root//*[jcr:contains(@propa, 'Lorem*')]", XPATH,
                ImmutableList.of("/test/k"));
        assertQuery("/jcr:root//*[jcr:contains(@propa, 'tru*')]", XPATH,
                ImmutableList.of("/test/e"));
    }
}
