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

package org.apache.jackrabbit.oak.segment.file.tar.binaries;

import java.util.Objects;

class Generation {

    final int generation;

    final int full;

    final boolean compacted;

    Generation(int generation, int full, boolean compacted) {
        this.generation = generation;
        this.full = full;
        this.compacted = compacted;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (getClass() != o.getClass()) {
            return false;
        }
        return equals((Generation) o);
    }

    private boolean equals(Generation that) {
        return generation == that.generation && full == that.full && compacted == that.compacted;
    }

    @Override
    public int hashCode() {
        return Objects.hash(generation, full, compacted);
    }

    @Override
    public String toString() {
        return String.format(
            "Generation{generation=%d, full=%d, compacted=%s}",
            generation,
            full,
            compacted
        );
    }

}
