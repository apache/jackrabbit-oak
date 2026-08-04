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
package org.apache.jackrabbit.oak.security.audit;

import org.apache.jackrabbit.oak.spi.audit.AuditDomain;
import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditEventEmitter;
import org.apache.jackrabbit.oak.spi.audit.AuditEvents;
import org.jetbrains.annotations.NotNull;
import org.osgi.service.component.annotations.Component;

/**
 * OSGi service implementation of {@link AuditEventEmitter}. Thin wrapper
 * around the static {@link AuditEvents} façade so consumer bundles do not
 * need to know about the façade.
 * <p>
 * A single instance is registered per OSGi container at activation of
 * the audit module; all consumers receive the same instance via
 * {@code @Reference AuditEventEmitter}.
 */
@Component(service = AuditEventEmitter.class)
public class AuditEventEmitterImpl implements AuditEventEmitter {

    @Override
    public void emit(@NotNull AuditEvent event) {
        AuditEvents.dispatch(event);
    }

    @Override
    public boolean isEnabledFor(@NotNull AuditDomain domain) {
        return AuditEvents.isEnabledFor(domain);
    }
}
