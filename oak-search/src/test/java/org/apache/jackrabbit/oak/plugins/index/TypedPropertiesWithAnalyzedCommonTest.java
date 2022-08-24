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

public abstract class TypedPropertiesWithAnalyzedCommonTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    protected void assertEventually(Runnable r) {
        TestUtils.assertEventually(r, ((repositoryOptionsUtil.isAsync() ? repositoryOptionsUtil.defaultAsyncIndexingTimeInSeconds : 0) + 3000) * 5);
    }

    @Override
    protected void createTestIndexNode() throws Exception {
        setTraversalEnabled(false);
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

    @Test
    public void typeBooleanAnalyzed() throws Exception {
        IndexDefinitionBuilder builder = indexOptions.createIndexDefinitionBuilder();
        builder.noAsync();
        builder.indexRule("nt:base")
                .property("propa")
                .analyzed()
                .type("Boolean");
        setIndex(builder, UUID.randomUUID().toString());
        root.commit();

        addContent();
        runQueries();
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
        test.addChild("p1").setProperty("propa", 1234);
        test.addChild("p2").setProperty("propa", "1234");
        test.addChild("p3").setProperty("propa", "1234abcd");
        test.addChild("p4").setProperty("propa", "1234.12");
        test.addChild("p5").setProperty("propa", true);
        test.addChild("p6").setProperty("propa", "true");
        test.addChild("p7").setProperty("propa", 4321);
        test.addChild("p8").setProperty("propa", "4321");
        test.addChild("p9").setProperty("propa", "4321abcd");
        test.addChild("p10").setProperty("propa", "4321.12");
        test.addChild("p11").setProperty("propa", false);
        test.addChild("p12").setProperty("propa", "false");
        test.addChild("p13").setProperty("propa", "LoremIpsum");
        root.commit();
    }

    private void runQueries() {
        assertEventually(() -> {
            List<String> result = executeQuery("/jcr:root//*[jcr:contains(@propa, '123*')]", XPATH, true);
            assertFalse(result.isEmpty());
        });

        assertQuery("/jcr:root//*[jcr:contains(@propa, '123*')]", XPATH,
                ImmutableList.of("/test/p1", "/test/p2", "/test/p3", "/test/p4"));
        assertQuery("/jcr:root//*[jcr:contains(@propa, '432*')]", XPATH,
                ImmutableList.of("/test/p7", "/test/p8", "/test/p9", "/test/p10"));
        assertQuery("/jcr:root//*[jcr:contains(@propa, 'notpresent*')]", XPATH,
                ImmutableList.of());
        assertQuery("/jcr:root//*[jcr:contains(@propa, 'Lorem*')]", XPATH,
                ImmutableList.of("/test/p13"));
        assertQuery("/jcr:root//*[jcr:contains(@propa, 'tru*')]", XPATH,
                ImmutableList.of("/test/p5", "/test/p6"));
    }
}
