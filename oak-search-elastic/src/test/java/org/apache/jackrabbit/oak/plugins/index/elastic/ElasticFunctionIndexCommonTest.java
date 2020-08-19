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

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.oak.api.ContentRepository;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.FunctionIndexCommonTest;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.plugins.memory.PropertyStates;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Set;

import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.INDEX_DEFINITIONS_NODE_TYPE;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.REINDEX_PROPERTY_NAME;
import static org.apache.jackrabbit.oak.plugins.index.IndexConstants.TYPE_PROPERTY_NAME;

@Ignore
public class ElasticFunctionIndexCommonTest extends FunctionIndexCommonTest {
    // Set this connection string as
    // <scheme>://<hostname>:<port>?key_id=<>,key_secret=<>
    // key_id and key_secret are optional in case the ES server
    // needs authentication
    // Do not set this if docker is running and you want to run the tests on docker instead.
    private static String elasticConnectionString = System.getProperty("elasticConnectionString");
    @ClassRule
    public static ElasticConnectionRule elasticRule = new ElasticConnectionRule(elasticConnectionString);

    /*
    Close the ES connection after every test method execution
     */
    @After
    public void cleanup() throws IOException {
        elasticRule.closeElasticConnection();
    }

    public ElasticFunctionIndexCommonTest() {
        indexOptions = new ElasticIndexOptions();
    }

    @Override
    protected ContentRepository createRepository() {
        repositoryOptionsUtil = new ElasticTestRepositoryBuilder(elasticRule).build();
        return repositoryOptionsUtil.getOak().createContentRepository();
    }

    @Override
    protected void createTestIndexNode() {
        setTraversalEnabled(false);
    }

    protected Tree createIndex(String name, Set<String> propNames) {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }

    protected Tree createIndex(Tree index, String name, Set<String> propNames) {
        Tree def = index.addChild(INDEX_DEFINITIONS_NAME).addChild(name);
        def.setProperty(JcrConstants.JCR_PRIMARYTYPE,
                INDEX_DEFINITIONS_NODE_TYPE, Type.NAME);
        def.setProperty(TYPE_PROPERTY_NAME, indexOptions.getIndexType());
        def.setProperty(REINDEX_PROPERTY_NAME, true);
        def.setProperty(FulltextIndexConstants.FULL_TEXT_ENABLED, false);
        def.setProperty(PropertyStates.createProperty(FulltextIndexConstants.INCLUDE_PROPERTY_NAMES, propNames, Type.STRINGS));
        //def.setProperty(LuceneIndexConstants.SAVE_DIR_LISTING, true);
        return index.getChild(INDEX_DEFINITIONS_NAME).getChild(name);
    }

    @Override
    protected String getLoggerName() {
        return null;
    }
}
