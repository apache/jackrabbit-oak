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
package org.apache.jackrabbit.oak.plugins.migration.report;

import javax.annotation.Nonnull;

/**
 * A {@code Reporter} receives callbacks for every NodeState
 * and PropertyState that was accessed via a {ReportingNodeState}
 * instance.
 */
public interface Reporter {

    /**
     * Callback reporting that the given {@code nodeState} was accessed.
     *
     * @param nodeState The accessed {@code ReportingNodeState} instance.
     */
    void reportNode(@Nonnull final ReportingNodeState nodeState);

    /**
     * Callback reporting that the property named {@code propertyName}
     * was accessed on the {@code parent} node.
     *
     * @param parent The parent node state of the reported property.
     * @param propertyName The name of the reported property.
     */
    void reportProperty(@Nonnull final ReportingNodeState parent, @Nonnull final String propertyName);
}
