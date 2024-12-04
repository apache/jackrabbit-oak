/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.jackrabbit.oak.run.osgi;

import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT;
import static org.apache.jackrabbit.JcrConstants.NT_FILE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.query.Query;
import javax.jcr.query.QueryManager;
import javax.jcr.query.QueryResult;
import javax.jcr.query.Row;
import javax.jcr.query.RowIterator;
import org.apache.felix.connect.launch.PojoServiceRegistry;
import org.apache.jackrabbit.guava.common.collect.Lists;
import org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.index.search.IndexFormatVersion;
import org.apache.jackrabbit.oak.plugins.index.search.util.IndexHelper;
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer;
import org.apache.jackrabbit.oak.spi.query.QueryIndex.NodeAggregator;
import org.apache.jackrabbit.oak.spi.state.NodeBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LuceneSupportTest extends AbstractRepositoryFactoryTest {

    @Before
    public void setupRepo() {
        repositoryFactory = new CustomFactory();
        config.put(REPOSITORY_CONFIG_FILE, createConfigValue("oak-base-config.json", "oak-tar-config.json"));
    }

    @After
    public void logout() {
        session.logout();
    }

    @Test
    public void fullTextSearch() throws Exception {
        repository = repositoryFactory.getRepository(config);
        session = createAdminSession();

        Node testNode = session.getRootNode();
        Node contentNode = testNode.addNode("myFile", "nt:file").addNode("jcr:content", "oak:Unstructured");
        contentNode.setProperty("jcr:mimeType", "text/plain");
        contentNode.setProperty("jcr:encoding", "UTF-8");
        contentNode.setProperty("jcr:data",
                new ByteArrayInputStream("the quick brown fox jumps over the lazy dog.".getBytes("utf-8")));
        session.save();

        SimpleNodeAggregator agg = new SimpleNodeAggregator().newRuleWithName(NT_FILE,
                Lists.newArrayList(JCR_CONTENT, JCR_CONTENT + "/*"));
        getRegistry().registerService(NodeAggregator.class.getName(), agg, null);

        //The lucene index is set to synched mode
        AbstractRepositoryFactoryTest.retry(30, 200, () -> {
            try {
                String query = "SELECT * FROM [nt:base] as f WHERE CONTAINS (f.*, 'dog')";
                Set<String> queryResult = execute(query);
                assertTrue(Stream.of("/myFile", "/myFile/jcr:content").allMatch(queryResult::contains));
            } catch (RepositoryException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public Set<String> execute(String stmt) throws RepositoryException {
        QueryManager qm = session.getWorkspace().getQueryManager();
        Query query = qm.createQuery(stmt, "JCR-SQL2");
        QueryResult result = query.execute();
        RowIterator rowItr = result.getRows();
        final Set<String> paths = new HashSet<String>();
        while (rowItr.hasNext()) {
            Row r = rowItr.nextRow();
            paths.add(r.getNode().getPath());
        }
        return paths;
    }

    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    private Session session;

    private static class CustomFactory extends OakOSGiRepositoryFactory {

        @Override
        protected void preProcessRegistry(PojoServiceRegistry registry) {
            registry.registerService(RepositoryInitializer.class.getName(), new RepositoryInitializer() {
                @Override
                public void initialize(NodeBuilder builder) {
                    if (builder.hasChildNode(INDEX_DEFINITIONS_NAME)) {
                        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
                        if (!index.hasChildNode("lucene")) {
                            NodeBuilder nb = LuceneIndexHelper.newLuceneIndexDefinition(index,
                                    "lucene",
                                    IndexHelper.JR_PROPERTY_INCLUDES,
                                    null,
                                    "async");
                            nb.setProperty(FulltextIndexConstants.COMPAT_MODE, IndexFormatVersion.V1.getVersion());
                        }

                    }

                }

            }, null);
        }

    }
}
