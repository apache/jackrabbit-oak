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
 * A repository revision represents an immutable state of the repository.
 * <p>
 * Having a revision allows you to perform repeatable reads, because it is
 * assured that the state referenced by the revision is immutable.
 * <p>
 * The revision also allows the system to reference a known state of the
 * repository when changes are committed. Since a revision references an
 * immutable state, committing some changes will create a new state of the
 * repository that will be referenced by a new revision.
 */
public interface RemoteRevision {

    /**
     * Returns a string representation of the revision. This representation can
     * be used as a reference by an external system and converted to a {@code
     * RemoteRevision} object using {@link RemoteSession#readRevision(String)}.
     *
     * @return A string representation of the revision.
     */
    String asString();

}
