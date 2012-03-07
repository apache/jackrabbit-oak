/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.mk.model;

import org.apache.jackrabbit.mk.util.StringUtils;

import java.util.Arrays;

/**
 *
 */
public class Id {

    private final byte[] raw;

    public Id(byte[] raw) {
        this.raw = raw;
    }

    /**
     * Creates an <code>Id</code> instance from its
     * string representation as returned by {@link #toString()}.
     * <p/>
     * The following condition holds true:
     * <pre>
     * Id someId = ...;
     * assert(Id.fromString(someId.toString()).equals(someId));
     * </pre>
     *
     * @param s a string representation of an <code>Id</code>
     * @return an <code>Id</code> instance
     */
    public static Id fromString(String s) {
        return new Id(StringUtils.convertHexToBytes(s));
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(raw);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Id) {
            Id other = (Id) obj;
            return Arrays.equals(raw, other.raw);
        }
        return false;
    }

    @Override
    public String toString() {
        return StringUtils.convertBytesToHex(raw);
    }

    public byte[] getBytes() {
        return raw;
    }
}
