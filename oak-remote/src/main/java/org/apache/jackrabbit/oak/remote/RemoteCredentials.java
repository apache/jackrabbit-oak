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

/**
 * This interface is a marker interface to be used to represent both an
 * authentication strategy and the information to enable that authentication
 * strategy.
 * <p>
 * In example, a {@code RemoteCredentials} object created to represent an
 * authentication strategy based on user name and password, may encapsulate the
 * user name and password to enable this authentication strategy when a session
 * to the repository is created.
 * <p>
 * To create instances of this interface, take a look at the methods defined in
 * {@link RemoteRepository}.
 */
public interface RemoteCredentials {

}
