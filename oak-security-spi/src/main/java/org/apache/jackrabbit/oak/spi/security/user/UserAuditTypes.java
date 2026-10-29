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
package org.apache.jackrabbit.oak.spi.security.user;

import org.apache.jackrabbit.oak.spi.audit.AuditEvent;
import org.apache.jackrabbit.oak.spi.audit.AuditType;
import org.apache.jackrabbit.oak.spi.security.audit.SecurityAuditDomain;

/**
 * Stable event types and payload keys for user-management audit
 * events. All events declared here share the
 * {@link SecurityAuditDomain#DOMAIN oak.security} domain.
 * <p>
 * Listener bundles discriminate user-management events by combining
 * {@code event.getDomain().equals(SecurityAuditDomain.DOMAIN)} with
 * {@code event.getType().equals(UserAuditTypes.MEMBER_ADDED)} (or another
 * constant declared here).
 * <p>
 * A single membership add or remove and a bulk one share the same
 * string ({@link #MEMBER_ADDED} / {@link #MEMBER_REMOVED}); a bulk change is
 * simply an event whose {@link #PAYLOAD_MEMBER_IDS} list holds more than one
 * entry. Consumers that need a bulk/single flag derive it from the list size
 * rather than from a distinct type.
 * <p>
 * Future sub-domains under {@code oak.security} (ACL, principal, token)
 * declare their own event-type classes alongside their respective
 * configuration packages.
 * <p>
 * <strong>Asymmetric exposure.</strong> This class is the read-side
 * vocabulary; the producer-side factories ({@code UserAuditEvents} in
 * {@code oak-core}) are package-private by design. The partition is
 * defense-in-depth — it raises the bar for casual forging of
 * Oak-user-management events but does not prevent it (an external bundle
 * can still call {@link AuditEvent#of(String, String, java.util.Map)}
 * directly with this domain + a type from this class). Listeners that
 * need to distinguish Oak-attested events from fire-and-forget emissions
 * MUST check the reserved {@code commit.*} keys in the payload — a reliable
 * signal for events delivered through Oak dispatch; see the trust contract
 * on {@link AuditEvent#getPayload()}.
 */
public final class UserAuditTypes {

    // ── Type strings ──────────────────────────────────────────────────

    /**
     * Recorded when one or more authorizables are added as members of a
     * group. A single {@code Group.addMember} and a bulk
     * {@code Group.addMembers} share this type; discriminate by the size of
     * {@link #PAYLOAD_MEMBER_IDS}.
     * <p>
     * Payload keys: {@link #PAYLOAD_GROUP_PATH}, {@link #PAYLOAD_MEMBER_IDS},
     * {@link #PAYLOAD_MEMBERSHIP_SOURCE}, {@link #PAYLOAD_IS_CONTENT_ID}, and —
     * for single-member changes — {@link #PAYLOAD_MEMBER_PATHS}; bulk changes
     * additionally carry {@link #PAYLOAD_FAILED_IDS}.
     */
    public static final AuditType MEMBER_ADDED = AuditType.of("membership.added");

    /**
     * Recorded when one or more authorizables are removed from a group.
     * Single and bulk removes share this type; discriminate by the size of
     * {@link #PAYLOAD_MEMBER_IDS}. Payload keys: same as {@link #MEMBER_ADDED}.
     */
    public static final AuditType MEMBER_REMOVED = AuditType.of("membership.removed");

    // ── Payload keys ──────────────────────────────────────────────────

    /** Group path. Value type: {@code String}. */
    public static final String PAYLOAD_GROUP_PATH = "groupPath";

    /**
     * Member identifiers added or removed. Value type: {@code List<String>};
     * always present with at least one entry. Entries are content ids (UUIDs
     * from {@code rep:members}) when {@link #PAYLOAD_IS_CONTENT_ID} is
     * {@code true}, otherwise authorizable ids.
     */
    public static final String PAYLOAD_MEMBER_IDS = "memberIds";

    /**
     * JCR paths of the members, when the producer resolved them. Value type:
     * {@code List<String>}. Present on single-member changes; the bulk path
     * carries ids only and omits this key.
     */
    public static final String PAYLOAD_MEMBER_PATHS = "memberPaths";

    /**
     * Oak membership storage model the change applied to. Value type:
     * {@code String}. User-management API capture always writes the group's
     * {@code rep:members}, so this key is {@link #MEMBERSHIP_SOURCE_STATIC}.
     */
    public static final String PAYLOAD_MEMBERSHIP_SOURCE = "membershipSource";

    /**
     * {@code true} when {@link #PAYLOAD_MEMBER_IDS} carries content ids
     * (UUIDs from {@code rep:members}); {@code false} when they are
     * authorizable ids. Value type: {@code Boolean}.
     */
    public static final String PAYLOAD_IS_CONTENT_ID = "isContentId";

    /**
     * Ids that failed to stage (already-member, not-found, etc.). Value type:
     * {@code List<String>}; may be empty but never null. Carried on the bulk
     * path only.
     */
    public static final String PAYLOAD_FAILED_IDS = "failedIds";

    // ── Membership-source values ──────────────────────────────────────

    /**
     * {@link #PAYLOAD_MEMBERSHIP_SOURCE} value for changes written to a
     * group's {@code rep:members} — the model produced by the user-management
     * API. Other storage models ({@code static-sharded}, {@code dynamic},
     * {@code dynamic-external}) are not produced by these capture sites.
     */
    public static final String MEMBERSHIP_SOURCE_STATIC = "static";

    private UserAuditTypes() {
        // constants
    }
}
