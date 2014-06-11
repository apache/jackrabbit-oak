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

import org.apache.jackrabbit.oak.plugins.index.aggregate.NodeAggregator
import org.apache.jackrabbit.oak.plugins.index.aggregate.SimpleNodeAggregator
import org.junit.Before
import org.junit.Test

import javax.jcr.Session
import javax.jcr.Node
import javax.jcr.query.Query
import javax.jcr.query.QueryManager
import javax.jcr.query.QueryResult
import javax.jcr.query.Row
import javax.jcr.query.RowIterator

import static com.google.common.collect.Lists.newArrayList
import static org.apache.jackrabbit.JcrConstants.JCR_CONTENT
import static org.apache.jackrabbit.JcrConstants.NT_FILE
import static org.apache.jackrabbit.oak.run.osgi.OakOSGiRepositoryFactory.REPOSITORY_CONFIG_FILE


class LuceneSupportTest extends AbstractRepositoryFactoryTest {
    Session session

    @Before
    void setupRepo() {
        config[REPOSITORY_CONFIG_FILE] = createConfigValue("oak-base-config.json", "oak-tar-config.json")
    }

    @Test
    public void fullTextSearch() throws Exception {
        repository = repositoryFactory.getRepository(config)
        session = createAdminSession()

        String h = "Hello" + System.currentTimeMillis();
        String w = "World" + System.currentTimeMillis();

        Node testNode = session.getRootNode().addNode("test", "nt:folder")
        testNode.addNode("a" ,"nt:file")
                .addNode("jcr:content", "oak:Unstructured")
                .setProperty("name", [h,w] as String[])
        testNode.addNode("b", "nt:folder")
                .addNode("c", "nt:file")
                .addNode("jcr:content", "oak:Unstructured")
                .setProperty("name", h)
        session.save()

        String query = "/jcr:root//*[jcr:contains(., '$h')]"
        assert ["/test/a/jcr:content", "/test/b/c/jcr:content"] as HashSet == execute(query)

        SimpleNodeAggregator agg = new SimpleNodeAggregator().newRuleWithName(
                NT_FILE, newArrayList(JCR_CONTENT));
        getRegistry().registerService(NodeAggregator.class.name, agg, null)

        //TODO Need to have a query which makes use of aggregates
        //assert ["/test/a", "/test/b/c"] as HashSet == execute(query)

    }

    Set<String> execute(String stmt){
        QueryManager qm = session.workspace.queryManager
        Query query = qm.createQuery(stmt, "xpath");
        QueryResult result = query.execute()
        RowIterator rowItr = result.getRows()
        Set<String> paths = new HashSet<>()
        rowItr.each {Row r -> paths << r.node.path}
        return paths
    }
}
