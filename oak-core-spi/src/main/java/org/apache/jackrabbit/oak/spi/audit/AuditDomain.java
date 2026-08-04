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
 * Identifies the area of Oak (or of a consumer bundle) that produced an
 * audit event. Listeners subscribe per domain, and the pipeline routes
 * events by comparing domains, so this is the primary dispatch key.
 * <p>
 * The value is constrained at construction rather than at use: a domain is
 * usable as a JCR node name, so a listener that persists events into the
 * repository can build a path from it without escaping. See
 * {@link #of(String)} for the exact rules.
 * <p>
 * The set of domains is open. Oak's own areas declare constants in their
 * respective SPI modules (for example
 * {@code org.apache.jackrabbit.oak.spi.security.audit.SecurityAuditDomain}),
 * and consumer bundles are free to define their own. The convention is a
 * dotted identifier prefixed with the owning layer, which is why this is
 * not an enum.
 *
 * @see AuditType
 * @see AuditEvent#getDomain()
 */
public final class AuditDomain {

    private final String name;

    private AuditDomain(@NotNull String name) {
        this.name = name;
    }

    /**
     * Returns a domain for the given name.
     * <p>
     * The name must be non-blank and usable as a JCR node name: it is
     * rejected if {@link org.apache.jackrabbit.oak.namepath.JcrNameParser}
     * would not accept it (which rules out {@code /}, {@code [}, {@code ]},
     * {@code |} and {@code *}), and additionally if it contains a colon.
     * A colon denotes a namespace prefix in JCR and carries no meaning for
     * a flat audit identifier, so it is rejected rather than silently
     * reinterpreted.
     *
     * @param name the domain name, non-null and non-blank.
     * @return a domain wrapping {@code name}.
     * @throws IllegalArgumentException if {@code name} is blank or is not
     *         usable as a JCR node name.
     */
    @NotNull
    public static AuditDomain of(@NotNull String name) {
        return new AuditDomain(AuditNames.validate(name, "domain"));
    }

    /**
     * @return the domain name, never blank.
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
        return (o instanceof AuditDomain) && name.equals(((AuditDomain) o).name);
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    /**
     * @return the domain name; equivalent to {@link #name()}.
     */
    @Override
    public String toString() {
        return name;
    }
}
