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
import org.apache.jackrabbit.oak.plugins.tree.TreeUtil;
import org.apache.jackrabbit.oak.spi.nodetype.NodeTypeConstants;

import javax.jcr.RepositoryException;

import static org.junit.Assert.assertTrue;

public class IndexedQueryMixinTest extends AbstractQueryTest {

    @Override
    public void before() throws Exception {
        super.before();

        TreeUtil.addMixin(node, "mix:title", root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "userId");
        node.setProperty("jcr:title", "title");
        TreeUtil.addMixin(subnode, "mix:title", root.getTree(NodeTypeConstants.NODE_TYPES_PATH), "userId");
        subnode.setProperty("jcr:title", "title");

        grantPropertyReadAccess("jcr:title");

        root.commit();
    }

    @Override
    void createIndexDefinition() throws RepositoryException {
        Tree oakIndex = root.getTree("/"+IndexConstants.INDEX_DEFINITIONS_NAME);
        assertTrue(oakIndex.exists());
        IndexUtils.createIndexDefinition(oakIndex, "test-index", false, new String[] {"jcr:title"}, "mix:title");
    }

    String getStatement() {
        return "SELECT * FROM [mix:title] WHERE [jcr:title] is not null";
    }
}