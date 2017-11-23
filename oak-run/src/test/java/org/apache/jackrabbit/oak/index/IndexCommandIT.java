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
import java.util.List;
import java.util.concurrent.TimeUnit;


import com.google.common.io.Files;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.plugins.index.IndexConstants;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.IndexRootDirectory;
import org.apache.jackrabbit.oak.plugins.index.lucene.directory.LocalIndexDir;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;
import org.junit.Test;

import static java.nio.charset.Charset.defaultCharset;
import static org.apache.jackrabbit.oak.spi.state.NodeStateUtils.getNode;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class IndexCommandIT extends AbstractIndexCommandTest {
    @Before
    public void setUp() {
        IndexCommand.setDisableExitOnError(true);
    }

    @Test
    public void dumpStatsAndInfo() throws Exception{
        createTestData(false);
        //Close the repository so as all changes are flushed
        fixture.close();

        IndexCommand command = new IndexCommand();

        File outDir = temporaryFolder.newFolder();
        String[] args = {
                "-index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
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
        createTestData(false);
        //Close the repository so as all changes are flushed
        fixture.close();

        IndexCommand command = new IndexCommand();

        File outDir = temporaryFolder.newFolder();
        String[] args = {
                "-index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
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

    @Test
    public void consistencyCheck() throws Exception{
        createTestData(false);
        //Close the repository so as all changes are flushed
        fixture.close();

        IndexCommand command = new IndexCommand();

        File outDir = temporaryFolder.newFolder();
        String[] args = {
                "--index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-out-dir="  + outDir.getAbsolutePath(),
                "--index-consistency-check",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                fixture.getDir().getAbsolutePath()
        };

        command.execute(args);

        File report = new File(outDir, IndexCommand.INDEX_CONSISTENCY_CHECK_TXT);

        assertFalse(new File(outDir, IndexCommand.INDEX_INFO_TXT).exists());
        assertFalse(new File(outDir, IndexCommand.INDEX_DEFINITIONS_JSON).exists());
        assertTrue(report.exists());

        assertThat(Files.toString(report, defaultCharset()), containsString("/oak:index/fooIndex"));
    }

    @Test
    public void dumpIndex() throws Exception{
        createTestData(false);
        //Close the repository so as all changes are flushed
        fixture.close();

        IndexCommand command = new IndexCommand();

        File outDir = temporaryFolder.newFolder();
        String[] args = {
                "--index-temp-dir=" + temporaryFolder.newFolder().getAbsolutePath(),
                "--index-out-dir="  + outDir.getAbsolutePath(),
                "--index-dump",
                "--", // -- indicates that options have ended and rest needs to be treated as non option
                fixture.getDir().getAbsolutePath()
        };

        command.execute(args);
        File dumpDir = new File(outDir, IndexDumper.INDEX_DUMPS_DIR);
        assertTrue(dumpDir.exists());
    }
}