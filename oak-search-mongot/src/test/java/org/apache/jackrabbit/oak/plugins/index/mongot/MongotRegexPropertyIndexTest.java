/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.plugins.index.mongot;

import java.util.List;

import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class MongotRegexPropertyIndexTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    @After
    public void useFreshDatabase() {
        mongo.useFreshDatabase();
    }

    @Test
    public void regexProperty() throws Exception {
        MongotTestRepositoryBuilder builder = new MongotTestRepositoryBuilder(mongo);
        regexProperty(builder.definition());

        NodeBuilder test = builder.root().child("test");
        test.child("a").setProperty("propa", "foo");
        test.child("b").setProperty("propa", "foo");
        test.child("c").setProperty("propa", "foo2");
        test.child("d").setProperty("propc", "foo");
        test.child("e").setProperty("propd", "foo2");
        test.child("f").setProperty("propd", "foo1");
        for (int i = 0; i < 10_000; i++) {
            test.child("node" + i).setProperty("prop" + i, "foo");
        }

        try (MongotTestRepositoryBuilder.Fixture repository = builder.build()) {
            String query = "select [jcr:path] from [nt:base] where [propa] = 'foo'";
            repository.assertMongotPlan(query, "JCR-SQL2");
            assertEquals(List.of("/test/a", "/test/b"), repository.paths(query, "JCR-SQL2"));
        }
    }

    @Test
    public void regexPropertyOrderBy() throws Exception {
        MongotTestRepositoryBuilder builder = new MongotTestRepositoryBuilder(mongo);
        regexProperty(builder.definition());
        NodeBuilder test = builder.root().child("test");
        test.child("e").setProperty("propd", "foo2");
        test.child("f").setProperty("propd", "foo1");

        try (MongotTestRepositoryBuilder.Fixture repository = builder.build()) {
            String query = "select [jcr:path] from [nt:base] where [propd] like 'foo%' "
                    + "order by [propd]";
            repository.assertMongotPlan(query, "JCR-SQL2");
            assertEquals(List.of("/test/f", "/test/e"), repository.paths(query, "JCR-SQL2"));
        }
    }

    private static void regexProperty(IndexDefinitionBuilder builder) {
        IndexDefinitionBuilder.PropertyRule property =
                builder.indexRule("nt:base").property("allProperties");
        property.getBuilderTree().setProperty(FulltextIndexConstants.PROP_IS_REGEX, true);
        property.getBuilderTree().setProperty(
                FulltextIndexConstants.PROP_NAME, FulltextIndexConstants.REGEX_ALL_PROPS);
        property.propertyIndex().nodeScopeIndex();
    }
}
