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
package org.apache.jackrabbit.oak.plugins.index.solr.util;

import java.io.File;

import javax.jcr.Repository;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.SimpleCredentials;

import org.apache.jackrabbit.api.JackrabbitRepository;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Testcase for {@link org.apache.jackrabbit.oak.plugins.index.solr.util.NodeTypeIndexingUtils}
 */
public class NodeTypeIndexingUtilsTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder(new File("target"));

    private Repository r;
    private Session s;

    @Before
    public void setUp() throws RepositoryException {
        r = new Jcr().createRepository();
        s = r.login(new SimpleCredentials("admin", "admin".toCharArray()));
    }

    @After
    public void tearDown() {
        s.logout();
        s = null;
        if (r instanceof JackrabbitRepository) {
            ((JackrabbitRepository) r).shutdown();
        }
        r = null;
    }

    @Test
    public void testSynonymsFileCreation() throws Exception {
        File synonymsFile = NodeTypeIndexingUtils.createPrimaryTypeSynonymsFile(tempFolder.newFolder().getPath() +
                "/pt-synonyms.txt", s);
        assertNotNull(synonymsFile);
        assertTrue(synonymsFile.exists());
    }
}
