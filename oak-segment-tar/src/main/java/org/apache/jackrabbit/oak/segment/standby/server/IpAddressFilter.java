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
 * A white list for IP addresses. A filter can be a single IP address, a single
 * host name or a range of IP addresses.
 * <p>
 * Known issue: if a host name is provided as a filter and that host name
 * contains a dash ("-"), it will be interpreted as an IP range.
 */
class IpAddressFilter {

    private static final Logger log = LoggerFactory.getLogger(IpAddressFilter.class);

    private static long ipToLong(InetAddress ip) {
        byte[] octets = ip.getAddress();

        long result = 0;

        for (byte octet : octets) {
            result = (result << 8) | (octet & 0xff);
        }

        return result;
    }

    private static boolean isAllowed(InetAddress client, String left, String right) {
        InetAddress leftAddress;

        try {
            leftAddress = InetAddress.getByName(left);
        } catch (UnknownHostException e) {
            log.warn("Unable to resolve address or invalid IP literal " + left, e);
            return false;
        }

        InetAddress rightAddress;

        try {
            rightAddress = InetAddress.getByName(right);
        } catch (UnknownHostException e) {
            log.warn("Unable to resolve address or invalid IP literal " + right, e);
            return false;
        }

        return isAllowed(ipToLong(client), ipToLong(leftAddress), ipToLong(rightAddress));
    }

    private static boolean isAllowed(InetAddress client, String match) {
        InetAddress matchAddress;

        try {
            matchAddress = InetAddress.getByName(match);
        } catch (UnknownHostException e) {
            log.warn("Unable to resolve address or invalid IP literal " + match, e);
            return false;
        }

        return ipToLong(client) == ipToLong(matchAddress);
    }

    private static boolean isAllowed(long address, long left, long right) {
        return left <= address && address <= right;
    }

    private final String[] allowedIpRanges;

    /**
     * Create a new white list based on the provided filters.
     *
     * @param filters A list of filters.
     */
    IpAddressFilter(String[] filters) {
        this.allowedIpRanges = filters;
    }

    /**
     * Check if the provided IP address is allowed by this white list.
     *
     * @param address the address to verify.
     * @return {@code true} if the address is valid according to this white
     * list, {@code false} otherwise.
     */
    boolean isAllowed(InetAddress address) {
        if (allowedIpRanges == null) {
            return true;
        }

        if (allowedIpRanges.length == 0) {
            return true;
        }

        for (String s : this.allowedIpRanges) {
            int i = s.indexOf('-');

            if (i > 0) {
                if (isAllowed(address, s.substring(0, i).trim(), s.substring(i + 1).trim())) {
                    return true;
                }
            } else {
                if (isAllowed(address, s.trim())) {
                    return true;
                }
            }
        }

        return false;
    }

}
