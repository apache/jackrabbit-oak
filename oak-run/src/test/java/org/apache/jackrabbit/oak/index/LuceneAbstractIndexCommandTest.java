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

package org.apache.jackrabbit.oak.index;

import java.io.File;
import java.io.IOException;

import javax.jcr.Node;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.oak.plugins.index.lucene.util.LuceneIndexDefinitionBuilder;

import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;

public class LuceneAbstractIndexCommandTest extends AbstractIndexTestCommand {

    @Override
    protected IndexRepositoryFixture getRepositoryFixture(File dir) {
        return new LuceneRepositoryFixture(dir);
    }

    @Override
    protected void createIndex(String nodeType, String propName, boolean asyncIndex) throws IOException,
            RepositoryException {
        LuceneIndexDefinitionBuilder idxBuilder = new LuceneIndexDefinitionBuilder();
        if (!asyncIndex) {
            idxBuilder.noAsync();
        }
        idxBuilder.indexRule(nodeType).property(propName).propertyIndex();

        Session session = fixture.getAdminSession();
        Node fooIndex = getOrCreateByPath(TEST_INDEX_PATH,
                "oak:QueryIndexDefinition", session);

        idxBuilder.build(fooIndex);
        session.save();
        session.logout();
    }
}
