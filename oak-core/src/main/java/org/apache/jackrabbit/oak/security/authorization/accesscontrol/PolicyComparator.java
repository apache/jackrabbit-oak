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
package org.apache.jackrabbit.oak.security.authorization.accesscontrol;

import com.google.common.primitives.Ints;
import org.apache.jackrabbit.api.security.JackrabbitAccessControlPolicy;
import org.apache.jackrabbit.oak.commons.PathUtils;

import javax.jcr.security.AccessControlPolicy;
import java.util.Comparator;

final class PolicyComparator implements Comparator<AccessControlPolicy> {

    @Override
    public int compare(AccessControlPolicy policy1, AccessControlPolicy policy2) {
        if (policy1.equals(policy2)) {
            return 0;
        } else if (policy1 instanceof JackrabbitAccessControlPolicy && policy2 instanceof JackrabbitAccessControlPolicy) {
            return compare((JackrabbitAccessControlPolicy) policy1, (JackrabbitAccessControlPolicy) policy2);
        } else {
            if (policy1 instanceof JackrabbitAccessControlPolicy) {
                return -1;
            } else if (policy2 instanceof JackrabbitAccessControlPolicy) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    private static int compare(JackrabbitAccessControlPolicy policy1, JackrabbitAccessControlPolicy policy2) {
        String p1 = policy1.getPath();
        String p2 = policy2.getPath();

        if (p1 == null) {
            return -1;
        } else if (p2 == null) {
            return 1;
        } else {
            int depth1 = PathUtils.getDepth(p1);
            int depth2 = PathUtils.getDepth(p2);
            return (depth1 == depth2) ? p1.compareTo(p2) : Ints.compare(depth1, depth2);
        }
    }
}