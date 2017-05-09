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

package org.apache.jackrabbit.oak.remote;

import java.util.Set;

/**
 * The remote interface exposed by a repository.
 * <p>
 * Most methods require authentication to be used. As such, a client of this
 * interface is required to create an instance of {@code RemoteCredentials} and
 * use this instance to login into the repository and obtain a {@code
 * RemoteSession} object.
 */
public interface RemoteRepository {

    /**
     * Create a {@code RemoteCredentials} object representing an authentication
     * strategy based on a user name and a password. This kind of credentials
     * delegates authentication to the repository, using a user identified by
     * the user name and password provided to this method.
     *
     * @param user     User name.
     * @param password Password.
     * @return A {@code RemoteCredentials} object representing an authentication
     * strategy based on a user name and password.
     */
    RemoteCredentials createBasicCredentials(String user, char[] password);

    /**
     * Create a {@code RemoteCredentials} object representing an impersonation
     * authentication strategy. If this authentication strategy is used, the
     * repository will not make any attempt to perform authentication. It will
     * instead trust the information provided by the {@code RemoteCredentials}
     * and will create a {@code RemoteSession} bound to the principals specified
     * to this method.
     *
     * @param principals The set of principals to impersonate into.
     * @return A {@code RemoteCredentials} object representing an authentication
     * strategy based on impersonation.
     */
    RemoteCredentials createImpersonationCredentials(Set<String> principals);

    /**
     * Create a remote session exposing some repository operations.
     *
     * @param credentials An object representing an authentication strategy to
     *                    be used when invoking repository operations.
     * @return An instance of a remote session to invoke repository operations.
     * @throws RemoteLoginException if it was not possible to authenticate the
     *                              given credentials.
     */
    RemoteSession login(RemoteCredentials credentials) throws RemoteLoginException;

}
