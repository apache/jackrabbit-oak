/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jackrabbit.oak.remote.http.handler;

import org.apache.jackrabbit.oak.remote.RemoteRevision;
import org.apache.jackrabbit.oak.remote.RemoteSession;

import javax.servlet.http.HttpServletRequest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class SearchSpecificRevisionHandler extends SearchRevisionHandler {

    private static final Pattern REQUEST_PATTERN = Pattern.compile("^/revisions/([^/]+)/tree$");

    @Override
    protected RemoteRevision readRevision(HttpServletRequest request, RemoteSession session) {
        Matcher matcher = REQUEST_PATTERN.matcher(request.getPathInfo());

        if (matcher.matches()) {
            return session.readRevision(matcher.group(1));
        }

        throw new IllegalStateException("handler bound at the wrong path");
    }

}
