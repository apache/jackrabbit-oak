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
package org.apache.jackrabbit.oak.segment.standby;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.io.File;

import org.apache.jackrabbit.oak.commons.junit.TemporaryPort;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.SegmentTestUtils;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.apache.jackrabbit.oak.segment.test.TemporaryFileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class FailoverMultipleClientsTestIT extends TestBase {

    private TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private TemporaryFileStore serverFileStore = new TemporaryFileStore(folder, false);

    private TemporaryFileStore clientFileStore1 = new TemporaryFileStore(folder, true);

    private TemporaryFileStore clientFileStore2 = new TemporaryFileStore(folder, true);

    @Rule
    public TemporaryPort serverPort = new TemporaryPort();

    @Rule
    public RuleChain chain = RuleChain.outerRule(folder)
            .around(serverFileStore)
            .around(clientFileStore1)
            .around(clientFileStore2);

    @Test
    public void testMultipleClients() throws Exception {
        FileStore storeS = serverFileStore.fileStore();
        FileStore storeC = clientFileStore1.fileStore();
        FileStore storeC2 = clientFileStore2.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(storeS).build();
        try (
            StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), storeS, MB);
            StandbyClientSync cl1 = new StandbyClientSync(getServerHost(), serverPort.getPort(), storeC, false, getClientTimeout(), false, folder.newFolder());
            StandbyClientSync cl2 = new StandbyClientSync(getServerHost(), serverPort.getPort(), storeC2, false, getClientTimeout(), false, folder.newFolder())
        ) {
            serverSync.start();
            SegmentTestUtils.addTestContent(store, "server");
            storeS.flush();  // this speeds up the test a little bit...

            assertFalse("first client has invalid initial store!", storeS.getHead().equals(storeC.getHead()));
            assertFalse("second client has invalid initial store!", storeS.getHead().equals(storeC2.getHead()));
            assertEquals(storeC.getHead(), storeC2.getHead());

            cl1.run();
            cl2.run();

            assertEquals(storeS.getHead(), storeC.getHead());
            assertEquals(storeS.getHead(), storeC2.getHead());

            cl1.stop();
            SegmentTestUtils.addTestContent(store, "test");
            storeS.flush();
            cl1.run();
            cl2.run();

            assertEquals(storeS.getHead(), storeC2.getHead());
            assertFalse("first client updated in stopped state!", storeS.getHead().equals(storeC.getHead()));

            cl1.start();
            cl1.run();
            assertEquals(storeS.getHead(), storeC.getHead());
        }
    }

}
