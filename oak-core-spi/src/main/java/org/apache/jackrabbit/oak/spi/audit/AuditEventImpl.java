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

import java.util.Map;

import org.jetbrains.annotations.NotNull;

/**
 * Package-private immutable {@link AuditEvent} backing the
 * {@link AuditEvent#of(AuditDomain, AuditType, Map) AuditEvent.of} static
 * factories.
 * <p>
 * Not part of the SPI surface — consumers always see the bare
 * {@code AuditEvent} interface. Keeping the impl package-private is the
 * core of item 3 (drop typed event hierarchy): consumers cannot
 * {@code instanceof}-check or downcast; discrimination is via
 * {@link AuditEvent#getDomain()} + {@link AuditEvent#getType()}.
 * <p>
 * The {@code payload} Map is expected to already be immutable (the factory
 * runs {@link Map#copyOf} before constructing the impl); the constructor
 * stores it by reference.
 */
final class AuditEventImpl implements AuditEvent {

    private final AuditDomain domain;
    private final AuditType type;
    private final long timestamp;
    private final Map<String, Object> payload;

    AuditEventImpl(@NotNull AuditDomain domain,
                   @NotNull AuditType type,
                   long timestamp,
                   @NotNull Map<String, Object> payload) {
        this.domain = domain;
        this.type = type;
        this.timestamp = timestamp;
        this.payload = payload; // factory invariant: already an immutable Map
    }

    @NotNull
    @Override
    public AuditDomain getDomain() {
        return domain;
    }

    @NotNull
    @Override
    public AuditType getType() {
        return type;
    }

    @Override
    public long getTimestamp() {
        return timestamp;
    }

    @NotNull
    @Override
    public Map<String, Object> getPayload() {
        return payload;
    }
}
