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
 * The {@code AuthInfo} TODO... used for identification, authorization....
 */
public interface AuthInfo {

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

}
