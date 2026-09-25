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
package org.apache.jackrabbit.oak.spi.security.audit;

import org.apache.jackrabbit.oak.spi.audit.AuditDomain;

/**
 * Domain constant for events produced by Oak security modules.
 * <p>
 * Sub-domains under {@code oak.security} (user, ACL, principal, token)
 * share this single domain string and discriminate via the event
 * {@code type} field — see {@link org.apache.jackrabbit.oak.spi.security.user.UserAuditTypes}
 * for the user-management type vocabulary. Other Oak areas (e.g.
 * indexing, query, blob) declare their own domain-constant classes in
 * their respective SPI modules — not here.
 */
public final class SecurityAuditDomain {

    /**
     * Domain for events produced by Oak security modules (user management,
     * ACLs, principal management, tokens, etc.). Namespaced with the
     * {@code oak.} prefix so listeners hosted alongside other layers can
     * tell Oak's security events apart from same-named domains defined
     * elsewhere.
     */
    public static final AuditDomain DOMAIN = AuditDomain.of("oak.security");

    private SecurityAuditDomain() {
        // constants class
    }
}
