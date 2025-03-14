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

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.elastic.util.ElasticIndexUtils;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder.PropertyRule;
import org.junit.Test;

public class ElasticRegexPropertyIndexTest extends ElasticAbstractQueryTest {

    @Test
    public void regexPropertyWithFlattened() throws Exception {
        IndexDefinitionBuilder builder = createIndex("allProperties");
        PropertyRule prop = builder.indexRule("nt:base").property("allProperties");
        prop.getBuilderTree().setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);
        prop.getBuilderTree().setProperty(FulltextIndexConstants.PROP_NAME, "^[^\\/]*$");
        prop.nodeScopeIndex();
        prop.getBuilderTree().setProperty(ElasticPropertyDefinition.PROP_IS_FLATTENED, true);
        
        setIndex("test1", builder);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "foo");
        test.addChild("b").setProperty("propa", "foo");
        test.addChild("c").setProperty("propa", "foo2");
        test.addChild("d").setProperty("propc", "foo");
        test.addChild("e").setProperty("propd", "foo2");
        test.addChild("f").setProperty("propd", "foo1");

        // create 10k nodes with different property names to have high cardinality;
        // without flattened fields, this will break the test with
        // "Limit of total fields [1000] has been exceeded"
        for (int i = 0; i < 10_000; i++) {
            test.addChild("node" + i).setProperty("prop" + i, "foo");
        }
        root.commit();

        String propaQuery = "select [jcr:path] from [nt:base] where [propa] = 'foo'";

        assertEventually(() -> {
            String explain = explain(propaQuery);
            assertThat(explain, containsString("elasticsearch:test1"));
            assertThat(explain, containsString("[{\"term\":{\"flat:" +
                    ElasticIndexUtils.fieldName("allProperties") + "." +
                    ElasticIndexUtils.fieldName("propa") +
                    "\":{\"value\":\"foo\"}}}]"));
            assertQuery(propaQuery, List.of("/test/a", "/test/b"));
        });

        String propaOrderQuery = "select [jcr:path] from [nt:base] where [propd] like 'foo%' order by [propd]";

        assertEventually(() -> {
            String explain = explain(propaOrderQuery);
            assertThat(explain, containsString("elasticsearch:test1"));
            assertThat(explain, containsString("\"query\":{\"bool\":{\"filter\":[{\"prefix\":{\"flat:" +
                    ElasticIndexUtils.fieldName("allProperties") + "." +
                    ElasticIndexUtils.fieldName("propd") +
                    "\":{\"value\":\"foo\"}}}]}}"));
            assertThat(explain, containsString("\"sort\":[{\"flat:" +
                    ElasticIndexUtils.fieldName("allProperties") + "." +
                    ElasticIndexUtils.fieldName("propd") +
                    "\":{\"order\":\"asc\"}},{\":path\":{\"order\":\"asc\"}}]"));
            assertThat(explain, containsString("sortOrder: [{ propertyName : propd, propertyType : UNDEFINED, order : ASCENDING }]"));
            assertQuery(propaOrderQuery, List.of("/test/f", "/test/e"));
        });

    }

    @Test
    public void regexPropertyWithoutFlattened() throws Exception {
        IndexDefinitionBuilder builder = createIndex("allProperties");
        PropertyRule prop = builder.indexRule("nt:base").property("allProperties");
        prop.getBuilderTree().setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);
        prop.getBuilderTree().setProperty(FulltextIndexConstants.PROP_NAME, "^[^\\/]*$");
        prop.nodeScopeIndex();
        prop.getBuilderTree().setProperty(ElasticPropertyDefinition.PROP_IS_FLATTENED, false);

        setIndex("test1", builder);
        root.commit();

        Tree test = root.getTree("/").addChild("test");
        test.addChild("a").setProperty("propa", "foo");
        test.addChild("b").setProperty("propa", "foo");
        test.addChild("c").setProperty("propa", "foo2");
        test.addChild("d").setProperty("propc", "foo");
        test.addChild("e").setProperty("propd", "foo");

        // create 10k nodes with different property names to have high cardinality;
        // without flattened fields, this will break the test with
        // "Limit of total fields [1000] has been exceeded"
        for (int i = 0; i < 10_000; i++) {
            test.addChild("node" + i).setProperty("prop" + i, "foo");
        }
        try {
            root.commit();
            fail();
        } catch (CommitFailedException e) {
            String msg = e.getMessage();
            assertTrue(msg, msg.contains("Failed to index the node"));
            // Typically, the root cause is "Limit of total fields [1000] has been exceeded"
            // but something this is suppressed, and so we can not have an assertion on it
        }
    }

}
