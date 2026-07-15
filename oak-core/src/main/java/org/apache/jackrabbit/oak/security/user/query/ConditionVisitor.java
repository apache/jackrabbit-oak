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
package org.apache.jackrabbit.oak.security.user.query;

import org.jetbrains.annotations.NotNull;

import javax.jcr.RepositoryException;

interface ConditionVisitor {

    void visit(@NotNull Condition.Node node);

    void visit(@NotNull Condition.PropertyValue condition) throws RepositoryException;

    void visit(@NotNull Condition.PropertyLike condition) throws RepositoryException;

    void visit(@NotNull Condition.PropertyExists condition) throws RepositoryException;

    void visit(@NotNull Condition.Contains condition);

    void visit(@NotNull Condition.Impersonation condition);

    void visit(@NotNull Condition.Not condition) throws RepositoryException;

    void visit(@NotNull Condition.And condition) throws RepositoryException;

    void visit(@NotNull Condition.Or condition) throws RepositoryException;
}