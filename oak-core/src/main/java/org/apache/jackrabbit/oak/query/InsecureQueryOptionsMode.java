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
package org.apache.jackrabbit.oak.query;

/**
 * Mode parameter passed from JCR to the Oak QueryEngine indicating the desired
 * behavior if a parsed statement declares any insecure query options.
 *
 * @since 1.60
 */
public enum InsecureQueryOptionsMode {
    /**
     * Exclude any insecure query options from the query plan and result.
     */
    IGNORE,

    /**
     * Throw a ParseException if an insecure query option is present in the statement.
     */
    DENY,

    /**
     * Include any insecure query options specified by the statement in the query plan and the result. This should only
     * be passed for principals with the rep:insecureQueryOptions repository privilege.
     */
    ALLOW
}
