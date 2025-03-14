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
package org.apache.jackrabbit.oak.plugins.index.elastic;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.JCR_DATA;
import static org.apache.jackrabbit.JcrConstants.JCR_PRIMARYTYPE;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.JcrConstants.NT_FOLDER;
import static org.apache.jackrabbit.oak.plugins.memory.BinaryPropertyState.binaryProperty;

import java.util.Calendar;
import java.util.List;

import org.apache.jackrabbit.oak.api.CommitFailedException;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexAggregationCommonTest;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;

public class ElasticIndexAggregationTest extends IndexAggregationCommonTest {

    @ClassRule
    public static final ElasticConnectionRule elasticRule = new ElasticConnectionRule();

    public ElasticIndexAggregationTest() {
        this.indexOptions = new ElasticIndexOptions();
        this.repositoryOptionsUtil = new ElasticTestRepositoryBuilder(elasticRule).build();
    }

    @Override
    protected ContentRepository createRepository() {
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Override
    @Test
    @Ignore("OAK-10599: Elasticsearch does not support compatibility mode")
    public void oak3371AggregateV1() throws CommitFailedException {
        super.oak3371AggregateV1();
    }

    @Test
    public void testChildNodeWithOrCompositePlan() throws Exception {
        Tree content = root.getTree("/").addChild("content");
        Tree folder = content.addChild("myFolder");
        folder.setProperty(JCR_PRIMARYTYPE, NT_FOLDER, Type.NAME);
        Tree file = folder.addChild("myFile");
        file.setProperty(JCR_PRIMARYTYPE, NT_FILE, Type.NAME);
        file.setProperty("jcr:title", "title");
        file.setProperty("jcr:description", "description");

        Tree resource = file.addChild(JCR_CONTENT);
        resource.setProperty(JCR_PRIMARYTYPE, "nt:resource", Type.NAME);
        resource.setProperty("jcr:lastModified", Calendar.getInstance());
        resource.setProperty("jcr:encoding", "UTF-8");
        resource.setProperty("jcr:mimeType", "text/plain");
        resource.setProperty(binaryProperty(JCR_DATA,
                "the quick brown fox jumps over the lazy dog."));

        root.commit();

        assertEventually(() -> {
            String matchContentAll = "//element(*, nt:folder)[(jcr:contains(., 'dog'))]";
            assertThat(explainXPath(matchContentAll), containsString(
                    "\"fields\":[\":fulltext\"],\"query\":\"dog\""));
            assertQuery(matchContentAll, "xpath", List.of("/content/myFolder"));

            String matchContentSimple = "//element(*, nt:folder)[(jcr:contains(myFile, 'dog'))]";
            assertThat(explainXPath(matchContentSimple), containsString(
                    "\"fields\":[\":fulltext\"],\"query\":\"dog\""));
            assertQuery(matchContentSimple, "xpath", List.of("/content/myFolder"));

            String matchContent = " //element(*, nt:folder)[(jcr:contains(myFile, 'dog') or jcr:contains(myFile/@jcr:title, 'invalid') or jcr:contains(myFile/@jcr:description, 'invalid'))]";
            assertThat(explainXPath(matchContent), containsString(
                    "\"fields\":[\":fulltext\"],\"query\":\"dog\""));
            assertQuery(matchContent, "xpath", List.of("/content/myFolder"));

            String matchTitle = " //element(*, nt:folder)[(jcr:contains(myFile, 'invalid') or jcr:contains(myFile/@jcr:title, 'title') or jcr:contains(myFile/@jcr:description, 'invalid'))]";
            assertThat(explainXPath(matchTitle), containsString(
                    "\"fields\":[\":fulltext\"],\"query\":\"invalid\""));
            assertQuery(matchTitle, "xpath", List.of("/content/myFolder"));

            String matchDesc = " //element(*, nt:folder)[(jcr:contains(myFile, 'invalid') or jcr:contains(myFile/@jcr:title, 'invalid') or jcr:contains(myFile/@jcr:description, 'description'))]";
            assertThat(explainXPath(matchDesc), containsString(
                    "\"fields\":[\":fulltext\"],\"query\":\"invalid\""));
            assertQuery(matchDesc, "xpath", List.of("/content/myFolder"));

            String matchNone = " //element(*, nt:folder)[(jcr:contains(myFile, 'invalid') or jcr:contains(myFile/@jcr:title, 'invalid') or jcr:contains(myFile/@jcr:description, 'invalid'))]";
            assertThat(explainXPath(matchNone), containsString(
                    "\"fields\":[\":fulltext\"],\"query\":\"invalid\""));
            assertQuery(matchNone, "xpath", List.of());

            String matchOnlyTitleOr = " //element(*, nt:folder)[(jcr:contains(myFile/@jcr:title, 'title') or jcr:contains(myFile/@jcr:title, 'unknown') )]";
            assertThat(explainXPath(matchOnlyTitleOr), containsString(
                    "\"fields\":[\"jcr:title\"],\"query\":\"title\""));
            assertQuery(matchOnlyTitleOr, "xpath", List.of("/content/myFolder"));
        });
    }

    protected String explainXPath(String query) {
        return explain(query, XPATH);
    }

    protected String explain(String query, String language) {
        String explain = "explain " + query;
        return executeQuery(explain, language).get(0);
    }

}
