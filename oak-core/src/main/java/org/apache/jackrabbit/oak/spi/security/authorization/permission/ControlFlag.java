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
package org.apache.jackrabbit.oak.spi.security.authorization.permission;

import javax.annotation.Nonnull;

/**
 * This class is used to define the evaluation behavior of a given aggregated
 * permission provider. It can be SUFFICIENT or REQUISITE.
 */
public enum ControlFlag {

    /**
     * The {@code AggregatedPermissionProvider} is not required to return {@code true}
     * upon permission evaluation. If it does grant the permissions in question,
     * control is immediately returned to the caller and the evaluation does
     * not proceed down the list of {@code PermissionProvider}s. If it returns
     * {@code false}, the evaluation continues down the list of {@code PermissionProvider}s.
     */
    SUFFICIENT("SUFFICIENT"),

    /**
     * The {@code AggregatedPermissionProvider} is required to return {@code true}
     * upon permission evaluation. If it grants access the evaluation continues
     * down the list of {@code PermissionProvider}s. However, if it returns
     * {@code false} indicating that permissions are not granted, the evaluation
     * is immediately stopped at this point control is returned to the caller
     * without proceeding down the list of the {@code PermissionProvider}s.
     */
    REQUISITE("REQUISITE");

    public static final String SUFFICIENT_NAME = "SUFFICIENT";
    public static final String REQUISITE_NAME = "REQUISITE";

    private final String name;

    private ControlFlag(@Nonnull String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return name;
    }
}
