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
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;

import com.google.common.io.Files;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.util.IndexDefinitionBuilder;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static java.nio.charset.Charset.defaultCharset;
import static org.apache.jackrabbit.commons.JcrUtils.getOrCreateByPath;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class IndexCommandIT {

    @Rule
    public final TemporaryFolder temporaryFolder = new TemporaryFolder(new File("target"));
    private RepositoryFixture fixture;

    @After
    public void cleaup() throws IOException {
        if (fixture != null) {
            fixture.close();
        }
    }

    @Test
    public void dumpStatsAndInfo() throws Exception{
        createTestData();
        //Close the repository so as all changes are flushed
        fixture.close();

        IndexCommand command = new IndexCommand();

        File outDir = temporaryFolder.newFolder();
        String[] args = {
                "-index-work-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "-index-out-dir="  + outDir.getAbsolutePath(),
                "-index-info",
                "-index-definitions",
                fixture.getDir().getAbsolutePath()
        };

        command.execute(args);

        File info = new File(outDir, IndexCommand.INDEX_INFO_TXT);
        File defns = new File(outDir, IndexCommand.INDEX_DEFINITIONS_JSON);

        assertTrue(info.exists());
        assertTrue(defns.exists());

        assertThat(Files.toString(info, defaultCharset()), containsString("/oak:index/uuid"));
        assertThat(Files.toString(info, defaultCharset()), containsString("/oak:index/fooIndex"));
    }

    @Test
    public void selectedIndexPaths() throws Exception{
        createTestData();
        //Close the repository so as all changes are flushed
        fixture.close();

        IndexCommand command = new IndexCommand();

        File outDir = temporaryFolder.newFolder();
        String[] args = {
                "-index-work-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "-index-out-dir="  + outDir.getAbsolutePath(),
                "-index-paths=/oak:index/fooIndex",
                "-index-info",
                "-index-definitions",
                fixture.getDir().getAbsolutePath()
        };

        command.execute(args);

        File info = new File(outDir, IndexCommand.INDEX_INFO_TXT);

        assertTrue(info.exists());

        assertThat(Files.toString(info, defaultCharset()), not(containsString("/oak:index/uuid")));
        assertThat(Files.toString(info, defaultCharset()), containsString("/oak:index/fooIndex"));
    }

    private void createTestData() throws IOException, RepositoryException {
        fixture = new RepositoryFixture(temporaryFolder.newFolder());
        indexIndexDefinitions();
        createLuceneIndex();
        addTestContent();
    }

    private void indexIndexDefinitions() throws IOException, RepositoryException {
        //By default Oak index definitions are not indexed
        //so add them to declaringNodeTypes
        Session session = fixture.getAdminSession();
        Node nodeType = session.getNode("/oak:index/nodetype");
        nodeType.setProperty(IndexConstants.DECLARING_NODE_TYPES, new String[] {"oak:QueryIndexDefinition"}, PropertyType.NAME);
        session.save();
        session.logout();
    }

    private void addTestContent() throws IOException, RepositoryException {
        Session session = fixture.getAdminSession();
        for (int i = 0; i < 100; i++) {
            getOrCreateByPath("/testNode/a"+i,
                    "oak:Unstructured", session).setProperty("foo", "bar");
        }
        session.save();
        session.logout();
    }

    private void createLuceneIndex() throws IOException, RepositoryException {
        IndexDefinitionBuilder idxBuilder = new IndexDefinitionBuilder();
        idxBuilder.noAsync();
        idxBuilder.indexRule("nt:base").property("foo").propertyIndex();

        Session session = fixture.getAdminSession();
        Node fooIndex = getOrCreateByPath("/oak:index/fooIndex",
                "oak:QueryIndexDefinition", session);

        idxBuilder.build(fooIndex);
        session.save();
        session.logout();
    }
}