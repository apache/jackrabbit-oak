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
package org.apache.jackrabbit.oak.benchmark;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

import org.apache.jackrabbit.api.JackrabbitSession;
import org.apache.jackrabbit.api.security.user.Group;
import org.apache.jackrabbit.api.security.user.User;
import org.apache.jackrabbit.api.security.user.UserManager;
import org.apache.jackrabbit.oak.spi.security.principal.PrincipalImpl;

/**
 * Microbenchmark isolating the per-event overhead of an audit capture
 * site firing on commit. Each iteration does {@code pairsPerIteration}
 * {@code addMember + save + removeMember + save} cycles on a fixed
 * group / user pair, i.e. {@code 2 * pairsPerIteration} commits per
 * iteration with exactly one audit capture-site fire per commit:
 * {@code UserAuditEvents.memberAdded(...)} resp. {@code UserAuditEvents.memberRemoved(...)}.
 *
 * Delta = (Oak-MemoryNS-Audit median) − (Oak-MemoryNS median), divided
 * by {@code 2 * pairsPerIteration}, is the per-event audit overhead
 * (allocation + path resolve + buffer record + drain + listener
 * dispatch). On the audit-OFF side capture sites short-circuit at
 * {@code AuditDispatch.isEnabled()} returning false (NOOP sink) so the
 * commit cost is the audit-free baseline.
 *
 * <p>Tunable via {@code -DpairsPerIteration=N} (default 50).
 */
public class AuditCaptureSiteOverheadTest extends AbstractTest<Object> {

    private static final int PAIRS_PER_ITERATION =
            Integer.getInteger("pairsPerIteration", 50);

    private static final String GROUP_ID = "auditBenchGroup_";
    private static final String USER_ID = "auditBenchUser_";

    private JackrabbitSession session;
    private UserManager userManager;
    private Group group;
    private User user;

    @Override
    public void beforeSuite() throws RepositoryException {
        session = (JackrabbitSession) loginWriter();
        userManager = session.getUserManager();
        group = userManager.createGroup(GROUP_ID + TEST_ID,
                new PrincipalImpl(GROUP_ID + TEST_ID), null);
        user = userManager.createUser(USER_ID + TEST_ID, null,
                new PrincipalImpl(USER_ID + TEST_ID), null);
        session.save();
    }

    @Override
    protected void runTest() throws Exception {
        for (int i = 0; i < PAIRS_PER_ITERATION; i++) {
            group.addMember(user);
            session.save();
            group.removeMember(user);
            session.save();
        }
    }

    @Override
    public void afterSuite() throws RepositoryException {
        try {
            if (group != null) group.remove();
            if (user != null) user.remove();
            session.save();
        } finally {
            logout(session);
        }
    }
}
