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
package org.apache.jackrabbit.oak.security.authorization.evaluation;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.IndexUtils;

import javax.jcr.RepositoryException;

import static org.junit.Assert.assertTrue;

public class IndexedQueryTest extends AbstractQueryTest {

    @Override
    public void before() throws Exception {
        super.before();

        node.setProperty("title", "a");
        subnode.setProperty("title", "b");

        grantPropertyReadAccess("title");

        root.commit();
    }

    @Override
    void createIndexDefinition() throws RepositoryException {
        Tree oakIndex = root.getTree("/"+IndexConstants.INDEX_DEFINITIONS_NAME);
        assertTrue(oakIndex.exists());
        IndexUtils.createIndexDefinition(oakIndex, "test-index", false, new String[] {"title"}, "nt:unstructured");
    }

    String getStatement() {
        return "SELECT * FROM [nt:unstructured] WHERE [title] is not null";
    }
}