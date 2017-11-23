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

package org.apache.jackrabbit.oak.run.osgi

import org.apache.felix.connect.launch.PojoServiceRegistry

import org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator
import org.apache.jackrabbit.oak.plugins.index.lucene.IndexFormatVersion
import org.apache.jackrabbit.oak.plugins.index.lucene.LuceneIndexConstants
import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexHelper
import org.apache.jackrabbit.oak.spi.lifecycle.RepositoryInitializer
import org.apache.jackrabbit.oak.spi.query.QueryIndex
import org.apache.jackrabbit.oak.spi.state.NodeBuilder
import org.junit.After
import org.junit.Before
import org.junit.Test

import javax.jcr.Node
import javax.jcr.Session
import javax.jcr.query.*

import static com.google.common.collect.Lists.newArrayList
import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT
import static org.apache.jackrabbit.JcrConstants.NT_FILE
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE

class LuceneSupportTest extends AbstractRepositoryFactoryTest {
    Session session

    @Before
    void setupRepo() {
        repositoryFactory = new CustomFactory()
        config[REPOSITORY_CONFIG_FILE] = createConfigValue("oak-base-config.json", "oak-tar-config.json")
    }

    @After
    void logout() {
        session.logout()
    }

    @Test
    public void fullTextSearch() throws Exception {
        repository = repositoryFactory.getRepository(config)
        session = createAdminSession()

        Node testNode = session.getRootNode()
        Node contentNode = testNode.addNode("myFile" ,"nt:file")
                .addNode("jcr:content", "oak:Unstructured");
        contentNode.setProperty("jcr:mimeType", "text/plain")
        contentNode.setProperty("jcr:encoding", "UTF-8")
        contentNode.setProperty("jcr:data",
                new ByteArrayInputStream("the quick brown fox jumps over the lazy dog.".getBytes('utf-8')))
        session.save()

        SimpleNodeAggregator agg = new SimpleNodeAggregator().newRuleWithName(
                NT_FILE, newArrayList(JCR_CONTENT, JCR_CONTENT + "/*"));
        getRegistry ( ).registerService ( QueryIndex.NodeAggregator.class.name, agg, null )

        //The lucene index is set to synched mode
        retry(30, 200) {
            String query = "SELECT * FROM [nt:base] as f WHERE CONTAINS (f.*, 'dog')"
            assert [ "/myFile", '/myFile/jcr:content' ] as HashSet == execute ( query )
            return true
        }
    }

    Set<String> execute(String stmt){
        QueryManager qm = session.workspace.queryManager
        Query query = qm.createQuery(stmt, "JCR-SQL2");
        QueryResult result = query.execute()
        RowIterator rowItr = result.getRows()
        Set<String> paths = new HashSet<>()
        rowItr.each {Row r -> paths << r.node.path}
        return paths
    }

    private static class CustomFactory extends OakOSGiRepositoryFactory {
        @Override
        protected void preProcessRegistry(PojoServiceRegistry registry) {
            registry.registerService(RepositoryInitializer.class.name, new RepositoryInitializer() {
                @Override
                void initialize(NodeBuilder builder) {
                    if (builder.hasChildNode(INDEX_DEFINITIONS_NAME)) {
                        NodeBuilder index = builder.child(INDEX_DEFINITIONS_NAME);
                        if (!index.hasChildNode("lucene")) {
                            NodeBuilder nb = LuceneIndexHelper.newLuceneIndexDefinition(
                                    index, "lucene", LuceneIndexHelper.JR_PROPERTY_INCLUDES, null, "async")
                            nb.setProperty(LuceneIndexConstants.COMPAT_MODE, IndexFormatVersion.V1.getVersion());
                        }
                    }
                }
            }, null);
        }
    }

}
