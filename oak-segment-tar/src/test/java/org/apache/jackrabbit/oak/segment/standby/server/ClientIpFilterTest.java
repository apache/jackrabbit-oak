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

package org.apache.jackrabbit.oak.segment.standby.server;

import static org.junit.Assert.assertEquals;

import java.net.InetAddress;
import java.net.InetSocketAddress;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ClientIpFilterTest {

    @Parameters(name = "filters={0}, address={1}, matches={2}")
    public static Object[] parameters() {
        return new Object[][] {
                {null, "127.0.0.1", true},
                {"", "127.0.0.1", true},
                {"127.0.0.1", "127.0.0.1", true},
                {"127.0.0.2", "127.0.0.1", false},
                {"::1", "::1", true},
                {"::2", "::1", false},
                {"localhost", "127.0.0.1", true},
                {"127.0.0.1-127.0.0.2", "127.0.0.1", true},
                {"127.0.0.0-127.0.0.1", "127.0.0.1", true},
                {"127.0.0.0-127.0.0.2", "127.0.0.1", true},
                {"127.0.0.2-127.0.0.1", "127.0.0.1", false},
                {"127-128, 126.0.0.1, 127.0.0.0-127.255.255.255", "127.0.0.1", true},
                {"122-126, ::1, 126.0.0.1, 127.0.0.0-127.255.255.255", "127.0.0.1", true},
                {"126.0.0.1, ::2, 128.0.0.1-255.255.255.255, 128.0.0.0-127.255.255.255", "127.0.0.1", false},
                {"127-128, 0:0:0:0:0:0:0:1, 126.0.0.1, 127.0.0.0-127.255.255.255", "::1", true},
                {"122-126, ::1, 126.0.0.1, 127.0.0.0-127.255.255.255", "::1", true},
                {"126.0.0.1, ::2, 128.0.0.1-255.255.255.255, 128.0.0.0-127.255.255.255", "::1", false},
        };
    }

    private final String addresses;

    private final String client;

    private final boolean match;

    public ClientIpFilterTest(String addresses, String client, boolean match) {
        this.addresses = addresses;
        this.client = client;
        this.match = match;
    }

    @Test
    public void test() throws Exception {
        assertEquals(match, new ClientIpFilter(parseFilters()).isAllowed(createAddress()));
    }

    private String[] parseFilters() {
        if (addresses == null) {
            return null;
        }

        return addresses.split(",");
    }

    private InetSocketAddress createAddress() throws Exception {
        return new InetSocketAddress(InetAddress.getByName(client), 8080);
    }

}
