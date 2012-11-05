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
import org.apache.jackrabbit.mongomk.model.CommitMongo;

import com.mongodb.DBCollection;
import com.mongodb.WriteResult;

/**
 * An action for saving a commit.
 */
public class SaveCommitAction extends AbstractAction<Boolean> {

    private final CommitMongo commitMongo;

    /**
     * Constructs a new {@code SaveCommitAction}.
     *
     * @param mongoConnection The {@link MongoConnection}.
     * @param commitMongo The {@link CommitMongo} to save.
     */
    public SaveCommitAction(MongoConnection mongoConnection, CommitMongo commitMongo) {
        super(mongoConnection);
        this.commitMongo = commitMongo;
    }

    @Override
    public Boolean execute() throws Exception {
        DBCollection commitCollection = mongoConnection.getCommitCollection();
        WriteResult writeResult = commitCollection.insert(commitMongo);
        if (writeResult.getError() != null) {
            throw new Exception(String.format("Insertion wasn't successful: %s", writeResult));
        }
        return Boolean.TRUE;
    }
}
