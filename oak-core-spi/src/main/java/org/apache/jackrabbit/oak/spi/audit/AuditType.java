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
package org.apache.jackrabbit.oak.spi.audit;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Identifies what happened, within the scope of an {@link AuditDomain}.
 * Types are only meaningful relative to their domain: two domains may use
 * the same type string for unrelated things, so listeners discriminate on
 * the pair.
 * <p>
 * Constrained the same way as {@link AuditDomain} — see {@link #of(String)}
 * — so an event's domain and type together can form a repository path
 * without escaping.
 * <p>
 * A distinct type from {@link AuditDomain} on purpose: the two are not
 * interchangeable, and keeping them separate lets the compiler catch a
 * domain passed where a type belongs.
 *
 * @see AuditEvent#getType()
 */
public final class AuditType {

    private final String name;

    private AuditType(@NotNull String name) {
        this.name = name;
    }

    /**
     * Returns a type for the given name.
     * <p>
     * Same rules as {@link AuditDomain#of(String)}: non-blank, usable as a
     * JCR node name, and no colon.
     *
     * @param name the type name, non-null and non-blank.
     * @return a type wrapping {@code name}.
     * @throws IllegalArgumentException if {@code name} is blank or is not
     *         usable as a JCR node name.
     */
    @NotNull
    public static AuditType of(@NotNull String name) {
        return new AuditType(AuditNames.validate(name, "type"));
    }

    /**
     * @return the type name, never blank.
     */
    @NotNull
    public String name() {
        return name;
    }

    @Override
    public boolean equals(@Nullable Object o) {
        if (this == o) {
            return true;
        }
        return (o instanceof AuditType) && name.equals(((AuditType) o).name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * @return the type name; equivalent to {@link #name()}.
     */
    @Override
    public String toString() {
        return name;
    }
}
