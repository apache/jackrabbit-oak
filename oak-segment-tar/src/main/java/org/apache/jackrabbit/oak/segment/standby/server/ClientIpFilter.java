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
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.UnknownHostException;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A white list for IP addresses. A filter can be a single IP address, a single
 * host name or a range of IP addresses.
 * <p>
 * Known issue: if a host name is provided as a filter and that host name
 * contains a dash ("-"), it will be interpreted as an IP range.
 */
class ClientIpFilter implements ClientFilter {

    private static final Logger log = LoggerFactory.getLogger(ClientIpFilter.class);

    private static boolean areAddressesEqual(InetAddress a, InetAddress b) {
        return Arrays.equals(a.getAddress(), b.getAddress());
    }

    private static int compare(byte[] left, byte[] right) {
        assert left.length == right.length;

        for (int i = 0; i < left.length; i++) {
            int l = left[i] & 0xff;
            int r = right[i] & 0xff;

            if (l < r) {
                return -1;
            }

            if (r < l) {
                return 1;
            }
        }

        return 0;
    }

    private static boolean isAddressInRange(InetAddress address, InetAddress left, InetAddress right) {
        byte[] addressBytes = address.getAddress();

        byte[] leftBytes = left.getAddress();

        if (leftBytes.length != addressBytes.length) {
            return false;
        }

        byte[] rightBytes = right.getAddress();

        if (rightBytes.length != addressBytes.length) {
            return false;
        }

        return compare(leftBytes, addressBytes) <= 0 && compare(addressBytes, rightBytes) <= 0;
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

        return isAddressInRange(client, leftAddress, rightAddress);
    }

    private static boolean isAllowed(InetAddress client, String match) {
        InetAddress matchAddress;

        try {
            matchAddress = InetAddress.getByName(match);
        } catch (UnknownHostException e) {
            log.warn("Unable to resolve address or invalid IP literal " + match, e);
            return false;
        }

        return areAddressesEqual(matchAddress, client);
    }

    private final String[] allowedIpRanges;

    /**
     * Create a new white list based on the provided filters.
     *
     * @param filters A list of filters.
     */
    ClientIpFilter(String[] filters) {
        this.allowedIpRanges = filters;
    }

    @Override
    public boolean isAllowed(SocketAddress address) {
        return isAllowed(((InetSocketAddress) address).getAddress());
    }

    private boolean isAllowed(InetAddress address) {
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
