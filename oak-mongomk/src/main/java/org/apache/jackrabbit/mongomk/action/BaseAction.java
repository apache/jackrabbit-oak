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
package org.apache.jackrabbit.mongomk.action;

import org.apache.jackrabbit.mongomk.impl.MongoConnection;

/**
 * An abstract base class for actions performed against {@code MongoDB}.
 *
 * @param <T> The result type of the query.
 */
public abstract class BaseAction<T> {

    protected MongoConnection mongoConnection;

    /**
     * Constructs a new {@code AbstractAction}.
     *
     * @param mongoConnection The mongo connection.
     */
    public BaseAction(MongoConnection mongoConnection) {
        this.mongoConnection = mongoConnection;
    }

    /**
     * Executes this action.
     *
     * @return The result of the action.
     * @throws Exception If an error occurred while executing the action.
     */
    public abstract T execute() throws Exception;
}
