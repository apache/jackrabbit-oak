/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.felix.jaas;

import javax.security.auth.spi.LoginModule;

import aQute.bnd.annotation.ConsumerType;

/**
 * TODO - Taken from Apache Felix JAAS as a temporary mesaure. Should
 * be removed once we have a released version of Felix JAAS Bundle
 *
 * A factory for creating {@link LoginModule} instances.
 */
@ConsumerType
public interface LoginModuleFactory
{
    /**
     * Property name specifying whether or not a <code>LoginModule</code> is
     * REQUIRED, REQUISITE, SUFFICIENT or OPTIONAL. Refer to {@link javax.security.auth.login.Configuration}
     * for more details around the meaning of these flags
     *
     * By default the value is set to REQUIRED
     */
    String JAAS_CONTROL_FLAG = "jaas.controlFlag";

    /**
     * Property name specifying the Realm name (or application name) against which the
     * LoginModule would be registered.
     *
     * <p>If no realm name is provided then LoginModule would registered with a default realm
     * as configured
     */
    String JAAS_REALM_NAME = "jaas.realmName";

    /**
     * Property name specifying the ranking (i.e. sort order) of the configured login module entries. The entries
     * are sorted in a descending order (i.e. higher value ranked configurations come first)
     * @since 1.0.1 (bundle version 0.0.2)
     */
    String JAAS_RANKING = "jaas.ranking";

    /**
     * Creates the LoginModule instance
     * @return loginModule instance
     */
    LoginModule createLoginModule();

}
