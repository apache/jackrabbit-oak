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

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple implementation of {@link AddressResolver} making use of
 * {@link InetAddress#getByName(String)} to resolve hosts to IP addresses.
 */
class InetAddressResolver implements AddressResolver {

    private static final Logger log = LoggerFactory.getLogger(InetAddressResolver.class);

    @Override
    public InetAddress resolve(String host) {
        InetAddress address;

        try {
            address = InetAddress.getByName(host);
        } catch (UnknownHostException e) {
            log.warn("Unable to resolve address or invalid IP literal " + host, e);
            address = null;
        }

        return address;
    }

}
