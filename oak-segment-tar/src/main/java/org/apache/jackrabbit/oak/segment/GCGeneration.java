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
 *
 */

package org.apache.jackrabbit.oak.segment;

import javax.annotation.Nonnull;

import com.google.common.base.Objects;

public final class GCGeneration {
    public static final GCGeneration NULL = new GCGeneration(0);

    private final int generation;

    public GCGeneration(int generation) {
        this.generation = generation;
    }

    public int getGeneration() {
        return generation;
    }

    public GCGeneration next() {
        return new GCGeneration(generation + 1);
    }

    public int compareWith(@Nonnull GCGeneration that) {
        return generation - that.generation;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (other == null || getClass() != other.getClass()) {
            return false;
        }
        GCGeneration that = (GCGeneration) other;
        return generation == that.generation;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(generation);
    }

    @Override
    public String toString() {
        return "GCGeneration{" +
                "generation=" + generation + '}';
    }
}
