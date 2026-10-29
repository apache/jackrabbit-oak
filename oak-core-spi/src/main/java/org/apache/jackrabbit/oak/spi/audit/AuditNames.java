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

import org.apache.jackrabbit.oak.namepath.JcrNameParser;
import org.jetbrains.annotations.NotNull;

/**
 * Shared validation for {@link AuditDomain} and {@link AuditType} names.
 * Package-private: the rules are exposed through the two factory methods,
 * not as API of their own.
 */
final class AuditNames {

    private AuditNames() {
        // utility class
    }

    /**
     * Validates an audit domain or type name.
     * <p>
     * {@code JcrNameParser} rejects {@code /}, {@code [}, {@code ]},
     * {@code |} and {@code *}, which is most of what we want. It does
     * accept a colon (it reads as a namespace prefix) and embedded
     * whitespace, and neither belongs in a flat audit identifier, so both
     * are rejected here on top of the parser's rules.
     *
     * @param name  the candidate name.
     * @param label {@code "domain"} or {@code "type"}, used in the message.
     * @return {@code name} unchanged, when valid.
     * @throws IllegalArgumentException if {@code name} is blank, contains a
     *         colon or whitespace, or is not usable as a JCR node name.
     */
    @NotNull
    static String validate(@NotNull String name, @NotNull String label) {
        if (name.isBlank()) {
            throw new IllegalArgumentException(label + " must not be blank");
        }
        if (name.indexOf(':') >= 0) {
            throw new IllegalArgumentException(
                    label + " must not contain ':' (reserved as a JCR namespace prefix): '" + name + "'");
        }
        for (int i = 0; i < name.length(); i++) {
            if (Character.isWhitespace(name.charAt(i))) {
                throw new IllegalArgumentException(
                        label + " must not contain whitespace: '" + name + "'");
            }
        }
        if (!JcrNameParser.validate(name)) {
            throw new IllegalArgumentException(
                    label + " must be usable as a JCR node name: '" + name + "'");
        }
        return name;
    }
}
