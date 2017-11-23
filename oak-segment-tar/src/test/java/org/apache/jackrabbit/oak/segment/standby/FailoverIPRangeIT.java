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

import static org.apache.jackrabbit.oak.commons.CIHelper.jenkinsNodeLabel;
import static org.apache.jackrabbit.oak.segment.SegmentTestUtils.addTestContent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assume.assumeFalse;

import java.io.File;

import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.apache.jackrabbit.oak.segment.standby.server.StandbyServerSync;
import org.apache.jackrabbit.oak.segment.test.TemporaryFileStore;
import org.apache.jackrabbit.oak.commons.junit.TemporaryPort;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TemporaryFolder;

public class FailoverIPRangeIT extends TestBase {

    private TemporaryFolder folder = new TemporaryFolder(new File("target"));

    private TemporaryFileStore serverFileStore = new TemporaryFileStore(folder, false);

    private TemporaryFileStore clientFileStore = new TemporaryFileStore(folder, true);

    @Rule
    public TemporaryPort serverPort = new TemporaryPort();

    @Rule
    public RuleChain chain = RuleChain.outerRule(folder)
            .around(serverFileStore)
            .around(clientFileStore);

    @Test
    public void testFailoverAllClients() throws Exception {
        createTestWithConfig(null, true);
    }

    @Test
    public void testFailoverLocalClient() throws Exception {
        createTestWithConfig(new String[] {"127.0.0.1"}, true);
    }

    @Test
    public void testFailoverLocalClientUseIPv6() throws Exception {
        assumeFalse(jenkinsNodeLabel("beam"));
        assumeFalse(noDualStackSupport);
        createTestWithConfig("::1", new String[] {"::1"}, true);
    }

    @Test
    public void testFailoverWrongClient() throws Exception {
        createTestWithConfig(new String[] {"127.0.0.2"}, false);
    }

    @Test
    public void testFailoverWrongClientIPv6() throws Exception {
        assumeFalse(jenkinsNodeLabel("beam"));
        assumeFalse(noDualStackSupport);
        createTestWithConfig(new String[] {"::2"}, false);
    }

    @Test
    public void testFailoverLocalhost() throws Exception {
        createTestWithConfig(new String[] {"localhost"}, true);
    }

    @Test
    public void testFailoverValidIPRangeStart() throws Exception {
        createTestWithConfig(new String[] {"127.0.0.1-127.0.0.2"}, true);
    }

    @Test
    public void testFailoverValidIPRangeEnd() throws Exception {
        createTestWithConfig(new String[] {"127.0.0.0-127.0.0.1"}, true);
    }

    @Test
    public void testFailoverValidIPRange() throws Exception {
        createTestWithConfig(new String[] {"127.0.0.0-127.0.0.2"}, true);
    }

    @Test
    public void testFailoverInvalidRange() throws Exception {
        createTestWithConfig(new String[] {"127.0.0.2-127.0.0.1"}, false);
    }

    @Test
    public void testFailoverCorrectList() throws Exception {
        createTestWithConfig(new String[] {"127-128", "126.0.0.1", "127.0.0.0-127.255.255.255"}, true);
    }

    @Test
    public void testFailoverCorrectListIPv6() throws Exception {
        assumeFalse(jenkinsNodeLabel("beam"));
        assumeFalse(noDualStackSupport);
        createTestWithConfig(new String[] {"122-126", "::1", "126.0.0.1", "127.0.0.0-127.255.255.255"}, true);
    }

    @Test
    public void testFailoverWrongList() throws Exception {
        createTestWithConfig(new String[] {"126.0.0.1", "::2", "128.0.0.1-255.255.255.255", "128.0.0.0-127.255.255.255"}, false);
    }

    @Test
    public void testFailoverCorrectListUseIPv6() throws Exception {
        assumeFalse(jenkinsNodeLabel("beam"));
        assumeFalse(noDualStackSupport);
        createTestWithConfig("::1", new String[] {"127-128", "0:0:0:0:0:0:0:1", "126.0.0.1", "127.0.0.0-127.255.255.255"}, true);
    }

    @Test
    public void testFailoverCorrectListIPv6UseIPv6() throws Exception {
        assumeFalse(jenkinsNodeLabel("beam"));
        assumeFalse(noDualStackSupport);
        createTestWithConfig("::1", new String[] {"122-126", "::1", "126.0.0.1", "127.0.0.0-127.255.255.255"}, true);
    }

    @Test
    public void testFailoverWrongListUseIPv6() throws Exception {
        assumeFalse(jenkinsNodeLabel("beam"));
        assumeFalse(noDualStackSupport);
        createTestWithConfig("::1", new String[] {"126.0.0.1", "::2", "128.0.0.1-255.255.255.255", "128.0.0.0-127.255.255.255"}, false);
    }

    private void createTestWithConfig(String[] ipRanges, boolean expectedToWork) throws Exception {
        createTestWithConfig("127.0.0.1", ipRanges, expectedToWork);
    }

    private void createTestWithConfig(String host, String[] ipRanges, boolean expectedToWork) throws Exception {
        FileStore storeS = serverFileStore.fileStore();
        FileStore storeC = clientFileStore.fileStore();

        NodeStore store = SegmentNodeStoreBuilders.builder(storeS).build();
        try (
                StandbyServerSync serverSync = new StandbyServerSync(serverPort.getPort(), storeS, 1 * MB, ipRanges);
                StandbyClientSync clientSync = new StandbyClientSync(host, serverPort.getPort(), storeC, false, getClientTimeout(), false)
        ) {
            serverSync.start();
            addTestContent(store, "server");
            storeS.flush();  // this speeds up the test a little bit...

            clientSync.run();

            if (expectedToWork) {
                assertEquals(storeS.getHead(), storeC.getHead());
            } else {
                assertFalse("stores are equal but shouldn't!", storeS.getHead().equals(storeC.getHead()));
            }
        }
    }

}
