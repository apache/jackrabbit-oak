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
package org.apache.jackrabbit.oak.spi.security.authentication.external.impl.principal;

import java.util.Set;

import com.google.common.collect.ImmutableSet;
import org.apache.jackrabbit.oak.spi.security.authentication.external.basic.DefaultSyncContext;

public interface ExternalIdentityConstants {

    String REP_EXTERNAL_ID = DefaultSyncContext.REP_EXTERNAL_ID;

    String REP_LAST_SYNCED = DefaultSyncContext.REP_LAST_SYNCED;

    String REP_EXTERNAL_PRINCIPAL_NAMES = "rep:externalPrincipalNames";

    Set<String> RESERVED_PROPERTY_NAMES = ImmutableSet.of(REP_EXTERNAL_ID, REP_EXTERNAL_PRINCIPAL_NAMES);

}