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
package org.apache.jackrabbit.oak.plugins.index;

import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.importer.IndexImporter;
import org.apache.jackrabbit.oak.plugins.index.search.FulltextIndexConstants;
import org.apache.jackrabbit.oak.query.AbstractQueryTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Set;

public abstract class IndexImporterReindexTest extends AbstractQueryTest {

    protected IndexOptions indexOptions;
    protected TestRepository repositoryOptionsUtil;

    @Test
    public void indexImporterFlagDeleteOnReindex() throws Exception {
        Tree luceneIndex = createIndex("test-index", Collections.<String>emptySet());
        Tree func = luceneIndex.addChild(FulltextIndexConstants.INDEX_RULES)
                .addChild("nt:base")
                .addChild(FulltextIndexConstants.PROP_NODE)
                .addChild("lowerLocalName");
        func.setProperty(FulltextIndexConstants.PROP_FUNCTION, "lower(localname())");

        root.commit();
        Assert.assertFalse(root.getTree("/oak:index/test-index").getProperty("reindex").getValue(Type.BOOLEAN));
        root.getTree("/oak:index/test-index").setProperty(IndexImporter.INDEX_IMPORT_STATE_KEY, "test", Type.STRING);
        root.getTree("/oak:index/test-index").setProperty("reindex", true, Type.BOOLEAN);
        root.commit();
        Assert.assertNull(root.getTree("/oak:index/test-index").getProperty(IndexImporter.INDEX_IMPORT_STATE_KEY));
    }

    protected Tree createIndex(String name, Set<String> propNames) {
        Tree index = root.getTree("/");
        return createIndex(index, name, propNames);
    }

    abstract protected Tree createIndex(Tree index, String name, Set<String> propNames);

    abstract protected String getLoggerName();
}
