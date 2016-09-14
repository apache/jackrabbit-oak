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
import java.net.UnknownHostException;

import org.junit.Test;

public class ClientIPFilterHostnameTest {

    @Test
    public void testInvalidHostname() throws Exception {
        String[] filters = new String[] {"foobar"};

        AddressResolver dummyAddressResolver = new AddressResolver() {

            @Override
            public InetAddress resolve(String host) {
                return null;
            }
        };

        ClientFilter clientFilter = new ClientIpFilter(filters, dummyAddressResolver);
        InetSocketAddress address = new InetSocketAddress("localhost", 8080);
        assertEquals(false, clientFilter.isAllowed(address));
    }

    @Test
    public void testHostnameWithDash() {
        String[] filters = new String[] {"foo-bar"};

        AddressResolver anythingToLocalhostResolver = new AddressResolver() {

            @Override
            public InetAddress resolve(String host) {
                InetAddress address = null;

                try {
                    if (host.equals("foo-bar")) {
                        address = InetAddress.getByName("localhost");
                    }
                } catch (UnknownHostException e) {
                    // ignore
                }

                return address;
            }
        };

        ClientFilter clientFilter = new ClientIpFilter(filters, anythingToLocalhostResolver);
        InetSocketAddress address = new InetSocketAddress("localhost", 8080);
        assertEquals(true, clientFilter.isAllowed(address));
    }

    @Test
    public void testHostnameWithMultipleDashes() {
        String[] filters = new String[] {"foo-bar-baz"};

        AddressResolver anythingToLocalhostResolver = new AddressResolver() {

            @Override
            public InetAddress resolve(String host) {
                InetAddress address = null;

                try {
                    if (host.equals("foo-bar-baz")) {
                        address = InetAddress.getByName("localhost");
                    }
                } catch (UnknownHostException e) {
                    // ignore
                }

                return address;
            }
        };

        ClientFilter clientFilter = new ClientIpFilter(filters, anythingToLocalhostResolver);
        InetSocketAddress address = new InetSocketAddress("localhost", 8080);
        assertEquals(true, clientFilter.isAllowed(address));
    }
}
