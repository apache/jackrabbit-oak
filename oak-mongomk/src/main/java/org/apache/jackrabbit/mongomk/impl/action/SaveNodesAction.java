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
package org.apache.jackrabbit.mongomk.impl.action;

import java.util.Collection;

import org.apache.jackrabbit.mongomk.impl.MongoNodeStore;
import org.apache.jackrabbit.mongomk.impl.model.MongoNode;

import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;

/**
 * An action for saving a list of nodes.
 */
public class SaveNodesAction extends BaseAction<Boolean> {

    private final Collection<MongoNode> nodeMongos;

    /**
     * Constructs a new {@code SaveNodesAction}.
     *
     * @param nodeStore Node store.
     * @param nodeMongos The list of {@link MongoNode}s.
     */
    public SaveNodesAction(MongoNodeStore nodeStore, Collection<MongoNode> nodeMongos) {
        super(nodeStore);
        this.nodeMongos = nodeMongos;
    }

    @Override
    public Boolean execute() throws Exception {
        DBCollection nodeCollection = nodeStore.getNodeCollection();
        DBObject[] temp = nodeMongos.toArray(new DBObject[nodeMongos.size()]);
        WriteResult writeResult = nodeCollection.insert(temp, WriteConcern.SAFE);
        if ((writeResult != null) && (writeResult.getError() != null)) {
            throw new Exception(String.format("Insertion wasn't successful: %s", writeResult));
        }
        return Boolean.TRUE;
    }
}
