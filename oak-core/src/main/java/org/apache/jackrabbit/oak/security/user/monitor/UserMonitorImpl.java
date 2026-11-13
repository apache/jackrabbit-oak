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
package org.apache.jackrabbit.oak.security.user.monitor;

import org.apache.jackrabbit.oak.stats.MeterStats;
import org.apache.jackrabbit.oak.stats.StatisticsProvider;
import org.apache.jackrabbit.oak.stats.StatsOptions;
import org.apache.jackrabbit.oak.stats.TimerStats;
import org.jetbrains.annotations.NotNull;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

public class UserMonitorImpl implements UserMonitor {

    private static final String ADD_MEMBERS_FAILED = "security.user.add_members.failed";
    private static final String ADD_MEMBERS_SUCCEEDED = "security.user.add_members.succeeded";
    private static final String ADD_MEMBERS_TIMER = "security.user.add_members.timer";

    private static final String REMOVE_MEMBERS_FAILED = "security.user.remove_members.failed";
    private static final String REMOVE_MEMBERS_SUCCEEDED = "security.user.remove_members.succeeded";
    private static final String  REMOVE_MEMBERS_TIMER = "security.user.remove_members.timer";

    private static final String  GET_MEMBERS_TIMER = "security.user.get_members.timer";
    private static final String  GET_DECLARED_MEMBERS_TIMER = "security.user.get_declared_members.timer";

    private static final String  MEMBEROF_TIMER = "security.user.memberof.timer";
    private static final String  DECLARED_MEMBEROF_TIMER = "security.user.declared_memberof.timer";

    private static final String ADMIN_LOGIN_SUCCEEDED = "security.user.admin.login.succeeded";
    private static final String ADMIN_LOGIN_FAILED = "security.user.admin.login.failed";
    private static final String ADMIN_IMPERSONATION_SUCCEEDED = "security.user.admin.impersonation.succeeded";
    private static final String ADMIN_IMPERSONATION_FAILED = "security.user.admin.impersonation.failed";
    private static final String ADMIN_PASSWORD_ADDED = "security.user.admin.password.added";
    private static final String ADMIN_PASSWORD_CHANGED = "security.user.admin.password.changed";
    private static final String ADMIN_IMPERSONATORS_GRANTED = "security.user.admin.impersonators.granted";
    private static final String ADMIN_IMPERSONATORS_REVOKED = "security.user.admin.impersonators.revoked";

    private final MeterStats addMembersFailed;
    private final MeterStats addMembersSucceeded;
    private final TimerStats addMembersTimer;

    private final MeterStats removeMembersFailed;
    private final MeterStats removeMembersSucceeded;
    private final TimerStats removeMembersTimer;

    private final TimerStats getMembersTimer;
    private final TimerStats getDeclaredMembersTimer;

    private final TimerStats memberOfTimer;
    private final TimerStats declaredMemberOfTimer;

    private final MeterStats adminLoginSucceeded;
    private final MeterStats adminLoginFailed;
    private final MeterStats adminImpersonationSucceeded;
    private final MeterStats adminImpersonationFailed;
    private final MeterStats adminPasswordAdded;
    private final MeterStats adminPasswordChanged;
    private final MeterStats adminImpersonatorsGranted;
    private final MeterStats adminImpersonatorsRevoked;

    public UserMonitorImpl(@NotNull StatisticsProvider statisticsProvider) {
        addMembersFailed = statisticsProvider.getMeter(ADD_MEMBERS_FAILED, StatsOptions.DEFAULT);
        addMembersSucceeded = statisticsProvider.getMeter(ADD_MEMBERS_SUCCEEDED, StatsOptions.DEFAULT);
        addMembersTimer = statisticsProvider.getTimer(ADD_MEMBERS_TIMER, StatsOptions.METRICS_ONLY);

        removeMembersFailed = statisticsProvider.getMeter(REMOVE_MEMBERS_FAILED, StatsOptions.DEFAULT);
        removeMembersSucceeded = statisticsProvider.getMeter(REMOVE_MEMBERS_SUCCEEDED, StatsOptions.DEFAULT);
        removeMembersTimer = statisticsProvider.getTimer(REMOVE_MEMBERS_TIMER, StatsOptions.METRICS_ONLY);

        getMembersTimer = statisticsProvider.getTimer(GET_MEMBERS_TIMER, StatsOptions.METRICS_ONLY);
        getDeclaredMembersTimer = statisticsProvider.getTimer(GET_DECLARED_MEMBERS_TIMER, StatsOptions.METRICS_ONLY);

        memberOfTimer = statisticsProvider.getTimer(MEMBEROF_TIMER, StatsOptions.METRICS_ONLY);
        declaredMemberOfTimer = statisticsProvider.getTimer(DECLARED_MEMBEROF_TIMER, StatsOptions.METRICS_ONLY);

        adminLoginSucceeded = statisticsProvider.getMeter(ADMIN_LOGIN_SUCCEEDED, StatsOptions.DEFAULT);
        adminLoginFailed = statisticsProvider.getMeter(ADMIN_LOGIN_FAILED, StatsOptions.DEFAULT);
        adminImpersonationSucceeded = statisticsProvider.getMeter(ADMIN_IMPERSONATION_SUCCEEDED, StatsOptions.DEFAULT);
        adminImpersonationFailed = statisticsProvider.getMeter(ADMIN_IMPERSONATION_FAILED, StatsOptions.DEFAULT);
        adminPasswordAdded = statisticsProvider.getMeter(ADMIN_PASSWORD_ADDED, StatsOptions.DEFAULT);
        adminPasswordChanged = statisticsProvider.getMeter(ADMIN_PASSWORD_CHANGED, StatsOptions.DEFAULT);
        adminImpersonatorsGranted = statisticsProvider.getMeter(ADMIN_IMPERSONATORS_GRANTED, StatsOptions.DEFAULT);
        adminImpersonatorsRevoked = statisticsProvider.getMeter(ADMIN_IMPERSONATORS_REVOKED, StatsOptions.DEFAULT);
    }

    @Override
    public void doneGetMembers(long timeTakenNanos, boolean declaredOnly) {
        if (declaredOnly) {
            getMembersTimer.update(timeTakenNanos, NANOSECONDS);
        } else {
            getDeclaredMembersTimer.update(timeTakenNanos, NANOSECONDS);
        }
    }

    @Override
    public void doneMemberOf(long timeTakenNanos, boolean declaredOnly) {
        if (declaredOnly) {
            declaredMemberOfTimer.update(timeTakenNanos, NANOSECONDS);
        } else {
            memberOfTimer.update(timeTakenNanos, NANOSECONDS);
        }
    }

    @Override
    public void doneUpdateMembers(long timeTakenNanos, long totalProcessed, long failed, boolean isRemove) {
        long successCnt = totalProcessed - failed;
        if (isRemove) {
            removeMembersFailed.mark(failed);
            removeMembersSucceeded.mark(successCnt);
            removeMembersTimer.update(timeTakenNanos, NANOSECONDS);
        } else {
            addMembersFailed.mark(failed);
            addMembersSucceeded.mark(successCnt);
            addMembersTimer.update(timeTakenNanos, NANOSECONDS);
        }
    }

    @Override
    public void adminLogin(boolean success) {
        (success ? adminLoginSucceeded : adminLoginFailed).mark();
    }

    @Override
    public void adminImpersonated(boolean success) {
        (success ? adminImpersonationSucceeded : adminImpersonationFailed).mark();
    }

    @Override
    public void adminPasswordChanged(boolean added) {
        (added ? adminPasswordAdded : adminPasswordChanged).mark();
    }

    @Override
    public void adminImpersonatorsChanged(long granted, long revoked) {
        adminImpersonatorsGranted.mark(granted);
        adminImpersonatorsRevoked.mark(revoked);
    }
}
