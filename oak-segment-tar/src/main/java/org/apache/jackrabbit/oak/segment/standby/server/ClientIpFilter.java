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
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 * A white list for IP addresses. A filter can be a single IP address, a single
 * host name or a range of IP addresses.
 */
class ClientIpFilter implements ClientFilter {

    private static final Pattern LETTERS = Pattern.compile("[a-zA-Z]+");

    private static boolean containsLetters(String s) {
        return LETTERS.matcher(s).matches();
    }

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

    private static boolean isAllowed(InetAddress client, AddressResolver addressResolver, String left, String right) {
        InetAddress leftAddress = addressResolver.resolve(left);
        InetAddress rightAddress = addressResolver.resolve(right);

        if (leftAddress == null || rightAddress == null) {
            return false;
        }

        return isAddressInRange(client, leftAddress, rightAddress);
    }

    private static boolean isAllowed(InetAddress client, AddressResolver addressResolver, String match) {
        InetAddress matchAddress = addressResolver.resolve(match);

        if (matchAddress == null) {
            return false;
        }

        return areAddressesEqual(matchAddress, client);
    }

    private final String[] allowedIpRanges;

    private final AddressResolver addressResolver;

    /**
     * Create a new white list based on the provided filters and the default
     * {@link InetAddressResolver}.
     *
     * @param filters A list of filters.
     */
    ClientIpFilter(String[] filters) {
        this(filters, new InetAddressResolver());
    }

    /**
     * Create a new white list based on the provided filters.
     *
     * @param filters         An array of filters.
     * @param addressResolver The resolver to be used for resolving IP
     *                        addresses.
     */
    ClientIpFilter(String[] filters, AddressResolver addressResolver) {
        this.allowedIpRanges = filters;
        this.addressResolver = addressResolver;
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
            String[] parts = s.split("-");

            if (parts.length == 2 && !containsLetters(parts[0]) && !containsLetters(parts[1])) {
                if (isAllowed(address, addressResolver, parts[0].trim(), parts[1].trim())) {
                    return true;
                }
            } else {
                if (isAllowed(address, addressResolver, s.trim())) {
                    return true;
                }
            }
        }

        return false;
    }

}
