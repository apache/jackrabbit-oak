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
package org.apache.jackrabbit.oak.plugins.index.mongot.query;

import java.util.List;

import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotSearchConnectionRule;
import org.apache.jackrabbit.oak.plugins.index.mongot.MongotTestRepositoryBuilder;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexDefinitionBuilder;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MongotFullTextQueryTest {

    @ClassRule
    public static final MongotSearchConnectionRule mongo = new MongotSearchConnectionRule();

    private static MongotTestRepositoryBuilder.Fixture repository;

    @BeforeClass
    public static void createRepository() throws Exception {
        MongotTestRepositoryBuilder builder = new MongotTestRepositoryBuilder(mongo);
        configure(builder.definition().indexRule("nt:base"));
        configure(builder.definition().indexRule("nt:unstructured"));
        NodeBuilder content = builder.root().child("content");
        content.child("a")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", "MongoDB native connector");
        content.child("b")
                .setProperty(JCR_PRIMARYTYPE, "nt:unstructured", Type.NAME)
                .setProperty("jcr:title", "Unrelated content");
        repository = builder.build();
    }

    @AfterClass
    public static void closeRepository() throws Exception {
        if (repository != null) {
            repository.close();
        }
    }

    @Test
    public void propertyScopedSql2AndXpathUseMongotIndex() throws Exception {
        String sql2 = "select [jcr:path], [jcr:score] from [nt:base] "
                + "where contains([jcr:title], 'mongodb') order by [jcr:score] desc";
        String plan = repository.query("explain " + sql2, "JCR-SQL2")
                .getRows().iterator().next().getValue("plan").getValue(Type.STRING);
        assertTrue(plan, plan.contains("mongot:"));
        assertEquals(List.of("/content/a"), repository.paths(sql2, "JCR-SQL2"));

        String xpath = "/jcr:root/content//*[jcr:contains(@jcr:title, 'mongodb')]";
        assertEquals(List.of("/content/a"), repository.paths(xpath, "xpath"));
    }

    @Test
    public void nodeScopedSql2UsesMongotIndex() throws Exception {
        String sql2 = "select [jcr:path] from [nt:base] where contains(*, 'connector')";
        assertEquals(List.of("/content/a"), repository.paths(sql2, "JCR-SQL2"));
    }

    private static void configure(IndexDefinitionBuilder.IndexRule rule) {
        rule.property("jcr:title").propertyIndex().analyzed().nodeScopeIndex();
    }
}
