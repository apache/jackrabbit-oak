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
package org.apache.jackrabbit.oak.api;

import javax.jcr.NoSuchWorkspaceException;
import javax.security.auth.login.LoginException;

/**
 * TODO:
 * The {@code RepositoryService} is the main access point of the oak-api. It
 * serves the following purposes:
 *
 * - validating a given login request and providing a connection
 *   that is used for further communication with the persistent layer (MK).
 *
 * The implementation of this and all related interfaces are intended to only
 * hold the state of the persistent layer at a given revision without any
 * session-related state modifications.
 */
public interface RepositoryService {
    Connection login(Object credentials, String workspaceName) throws LoginException, NoSuchWorkspaceException;
}