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
package org.apache.jackrabbit.mongomk.impl.command;

import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.action.FetchHeadRevisionIdAction;

/**
 * {@code Command} for {@code MongoMicroKernel#getHeadRevision()}
 */
public class GetHeadRevisionCommand extends BaseCommand<Long> {

    /**
     * Constructs a new {@code GetHeadRevisionCommandMongo}.
     *
     * @param nodeStore Node store.
     */
    public GetHeadRevisionCommand(MongoNodeStore nodeStore) {
        super(nodeStore);
    }

    @Override
    public Long execute() throws Exception {
        return new FetchHeadRevisionIdAction(nodeStore).execute();
    }
}