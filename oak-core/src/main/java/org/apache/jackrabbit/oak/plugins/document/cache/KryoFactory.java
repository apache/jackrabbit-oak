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

package org.apache.jackrabbit.oak.plugins.document.cache;

import java.util.NavigableMap;

import com.esotericsoftware.kryo.Kryo;
import org.apache.jackrabbit.oak.plugins.document.DocumentStore;
import org.apache.jackrabbit.oak.plugins.document.NodeDocument;
import org.apache.jackrabbit.oak.plugins.document.Revision;

/**
 * Factory to create Kryo instances customized for managing NodeDocument
 */
public class KryoFactory {

    public static Kryo createInstance(DocumentStore documentStore){
        Kryo kryo = new Kryo();
        kryo.setReferences(false);
        kryo.register(Revision.class, new Serializers.RevisionSerizlizer());
        kryo.register(NodeDocument.class, new Serializers.NodeDocumentSerializer(documentStore));
        kryo.register(NavigableMap.class);

        //All the required classes need to be registered explicitly
        kryo.setRegistrationRequired(true);
        return kryo;
    }
}
