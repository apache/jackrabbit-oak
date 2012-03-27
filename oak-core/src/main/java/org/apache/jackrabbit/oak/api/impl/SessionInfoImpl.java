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
package org.apache.jackrabbit.oak.api.impl;

import org.apache.jackrabbit.oak.api.SessionInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jcr.SimpleCredentials;

/**
 * SessionInfoImpl...
 */
public class SessionInfoImpl implements SessionInfo {

    /**
     * logger instance
     */
    private static final Logger log = LoggerFactory.getLogger(SessionInfoImpl.class);

    private final SimpleCredentials sc;
    private final String workspaceName;

    private String revision;

    SessionInfoImpl(SimpleCredentials sc, String workspaceName, String revision) {
        this.sc = sc;
        this.workspaceName = workspaceName;
        this.revision = revision;
    }

    @Override
    public String getUserID() {
        return sc.getUserID();
    }

    @Override
    public String[] getAttributeNames() {
        return sc.getAttributeNames();
    }

    @Override
    public Object getAttribute(String attributeName) {
        return sc.getAttribute(attributeName);
    }

    @Override
    public String getRevision() {
        return revision;
    }

    @Override
    public String getWorkspaceName() {
        return workspaceName;
    }

    @Override
    public void dispose() {
        // TODO

    }

    //--------------------------------------------------------------------------
    // TODO: tmp solution as long as oak-jcr still writes to MK directly
    public void setRevision(String revision) {
        this.revision = revision;
    }

}