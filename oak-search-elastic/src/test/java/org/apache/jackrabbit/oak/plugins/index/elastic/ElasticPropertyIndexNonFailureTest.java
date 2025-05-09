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

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.commons.junit.TemporarySystemProperty;
import org.apache.jackrabbit.oak.plugins.index.elastic.index.ElasticBulkProcessorHandler;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants.PROPDEF_PROP_NODE_NAME;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticPropertyIndexNonFailureTest extends ElasticAbstractQueryTest {
    @Rule
    public TemporarySystemProperty temporarySystemProperty = new TemporarySystemProperty();

    // Tests are hardcoded for these values
    private final static int BULK_ACTIONS_TEST = 250;
    private final static int BULK_SIZE_BYTES_TEST = 1024 * 1024;

    @Before
    public void before() throws Exception {
        // Use a low value for the tests
        System.setProperty(ElasticBulkProcessorHandler.BULK_ACTIONS_PROP, Integer.toString(BULK_ACTIONS_TEST));
        System.setProperty(ElasticBulkProcessorHandler.BULK_SIZE_BYTES_PROP, Integer.toString(BULK_SIZE_BYTES_TEST));
        super.before();
    }

    @Override
    protected boolean isInferenceEnabled() {
        return false;
    }

    /*
        In indexFailuresWithFailOnErrorOn test we are explicitly setting "strict mapping". For inference
     enabled oak, this is not supported as enricher for oak will be an external service and this enricher
     service would need some flexibility and may want to add additional properties.

     So we are explicitly setting inference to false for this test.
     */

    @Test
    public void indexFailuresWithFailOnErrorOn() throws Exception {
        if (ElasticPropertyDefinition.PROP_IS_FLATTENED_DEFAULT) {
            // if "flattened" enabled by default,
            // then the test doesn't make sense.
            // alternatively, disable "flattened" in the index definition;
            // but this is already tested in ElasticRegexPropertyIndexTest
            return;
        }
        IndexDefinitionBuilder builder = createIndex("a");
        builder.includedPaths("/test")
            .indexRule("nt:base")
            .property("nodeName", PROPDEF_PROP_NODE_NAME);

        // configuring the index with a regex property and strict mapping to simulate failures
        builder.indexRule("nt:base").property("b", true).propertyIndex();
        builder.getBuilderTree().setProperty(ElasticIndexDefinition.DYNAMIC_MAPPING, "strict");

        setIndex("test1", builder);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        for (int i = 1; i < 3; i++) {
            test.addChild("a" + i).setProperty("a", "foo");
        }
        root.commit();

        // now we add 5 correct docs and 5 docs cannot be mapped
        test.addChild("a100").setProperty("a", "foo");
        test.addChild("a200").setProperty("b", "foo");
        test.addChild("a101").setProperty("a", "foo");
        test.addChild("a201").setProperty("b", "foo");
        test.addChild("a102").setProperty("a", "foo");
        test.addChild("a202").setProperty("b", "foo");
        test.addChild("a103").setProperty("a", "foo");
        test.addChild("a203").setProperty("b", "foo");
        test.addChild("a104").setProperty("a", "foo");
        test.addChild("a204").setProperty("b", "foo");

        CommitFailedException cfe = null;
        try {
            root.commit();
        } catch (CommitFailedException e) {
            cfe = e;
        }

        assertThat("no exception thrown", cfe != null);
        assertThat("the exception cause has to be an IOException", cfe.getCause() instanceof IOException);
        assertThat("there should be 5 suppressed exception", cfe.getCause().getSuppressed().length == 5);

        String query = "select [jcr:path] from [nt:base] where [a] = 'foo'";
        assertEventually(() -> assertQuery(query, SQL2,
            List.of("/test/a1", "/test/a2", "/test/a100", "/test/a101", "/test/a102", "/test/a103", "/test/a104")
        ));
    }
}
