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

import static org.junit.Assume.assumeTrue;

import org.apache.commons.lang3.SystemUtils;
import org.apache.jackrabbit.oak.commons.CIHelper;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.standby.client.StandbyClientSync;
import org.junit.BeforeClass;

public class TestBase {
    static final int MB = 1024 * 1024;
    private static final int timeout = Integer.getInteger("standby.test.timeout", 500);

    // Java 6 on Windows doesn't support dual IP stacks, so we will skip our
    // IPv6 tests.
    final boolean noDualStackSupport = SystemUtils.IS_OS_WINDOWS && SystemUtils.IS_JAVA_1_6;

    @BeforeClass
    public static void assumptions() {
        assumeTrue(!CIHelper.travis());
    }

    static String getServerHost() {
        return "127.0.0.1";
    }

    static int getClientTimeout() {
        return timeout;
    }

    public StandbyClientSync newStandbyClientSync(FileStore store, int port) throws Exception {
        return newStandbyClientSync(store, port, false);
    }

    public StandbyClientSync newStandbyClientSync(FileStore store, int port, boolean secure) throws Exception {
        return new StandbyClientSync(getServerHost(), port, store, secure, getClientTimeout(), false);
    }

}
