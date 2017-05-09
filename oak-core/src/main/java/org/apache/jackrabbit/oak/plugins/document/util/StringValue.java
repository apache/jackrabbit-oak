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
package org.apache.jackrabbit.oak.plugins.document.util;

import javax.annotation.Nonnull;

import org.apache.jackrabbit.oak.cache.CacheValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A cache value wrapping a simple string.
 */
public final class StringValue implements CacheValue {

    private static final Logger log = LoggerFactory.getLogger(StringValue.class);

    private final String value;

    public StringValue(@Nonnull String value) {
        this.value = checkNotNull(value);
    }

    @Override
    public int getMemory() {
        return getMemory(value);
    }

    public static int getMemory(@Nonnull String s) {
        long size = 16                            // shallow size
                    + 40 + (long)s.length() * 2;  // value
        if (size > Integer.MAX_VALUE) {
            log.debug("Estimated memory footprint larger than Integer.MAX_VALUE: {}.", size);
            size = Integer.MAX_VALUE;
        }
        return (int) size;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof StringValue) {
            StringValue other = (StringValue) obj;
            return value.equals(other.value);
        }
        return false;
    }

    @Override
    public String toString() {
        return value;
    }

    public String asString() {
        return value;
    }
    
    public static StringValue fromString(String value) {
        return new StringValue(value);
    }

}
