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

/**
 * The {@code SessionInfo} TODO... describe how obtained, when disposed, used for communication with oak-api, identification, authorization....
 */
public interface SessionInfo {

    /**
     * Return the user ID to be exposed on the JCR Session object. It refers
     * to the ID of the user associated with the Credentials passed to the
     * repository login.
     *
     * @return the user ID such as exposed on the JCR Session object.
     */
    String getUserID();

    /**
     * Returns the attribute names associated with this instance.
     *
     * @return The attribute names with that instance or an empty array if
     * no attributes are present.
     */
    String[] getAttributeNames();

    /**
     * Returns the attribute with the given name or {@code null} if no attribute
     * with that {@code attributeName} exists.
     *
     * @param attributeName The attribute name.
     * @return The attribute or {@code null}.
     */
    Object getAttribute(String attributeName);

    /**
     * Returns the current revision the associated session is operating on.
     * Upon creation of a given {@code SessionInfo} instance the initial
     * revision is always set to the head revision. Upon successful commit
     * of changes the revision is reset to match the latest state.
     *
     * TODO: define how the desired initial revision is passed to the oak-api / mk
     * TODO: define how and when the revision is updated (would a setRevision required? and who would use it?)
     * TODO: define default value (head-revision?)
     *
     * @return the revision.
     */
    String getRevision();

    /**
     * The immutable name of the workspace this instance has been created for.
     *
     * @return name of the workspace this instance has been created for.
     */
    String getWorkspaceName();

    /**
     * Dispose this instance of {@code SessionInfo} as the associated
     * JCR Session instance was logged out. This method allows an implementation
     * to free any resources that may possibly be associated with this instance.
     */
    void dispose();
}