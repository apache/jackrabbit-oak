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
package org.apache.jackrabbit.oak.security.authorization.permission;

import com.google.common.base.Objects;
import org.jetbrains.annotations.NotNull;

final class NumEntries {

    static final NumEntries ZERO = new NumEntries(0, true);

    final long size;
    final boolean isExact;

    private NumEntries(long size, boolean isExact) {
        this.size = size;
        this.isExact = isExact;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(size, isExact);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof NumEntries) {
            NumEntries other = (NumEntries) obj;
            return size == other.size && isExact == other.isExact;
        } else {
            return false;
        }
    }

    @NotNull
    static NumEntries valueOf(long size, boolean isExact) {
        if (size == 0) {
            // if size is zero we assume that this is the correct value
            // irrespective of the isExact flag.
            return ZERO;
        } else {
            return new NumEntries(size, isExact);
        }
    }
}