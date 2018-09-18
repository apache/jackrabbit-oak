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

package org.apache.jackrabbit.oak.api.jmx;

import javax.management.MBeanOperationInfo;

public enum ImpactOption {
    /**
     * Indicates that the operation is a write-like,
     * and would modify the MBean in some way, typically by writing some value
     * or changing a configuration.
     */
    ACTION(MBeanOperationInfo.ACTION),
    /**
     * Indicates that the operation is both read-like and write-like.
     */
    ACTION_INFO(MBeanOperationInfo.ACTION_INFO),
    /**
     * Indicates that the operation is read-like,
     * it basically returns information.
     */
    INFO(MBeanOperationInfo.INFO),
    /**
     * Indicates that the operation has an "unknown" nature.
     */
    UNKNOWN(MBeanOperationInfo.UNKNOWN);

    public int value(){
        return value;
    }

    private final int value;

    private ImpactOption(int value) {
        this.value = value;
    }
}
