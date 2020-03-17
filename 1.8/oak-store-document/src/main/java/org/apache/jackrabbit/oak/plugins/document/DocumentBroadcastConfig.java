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
package org.apache.jackrabbit.oak.plugins.document;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.oak.plugins.document.persistentCache.broadcast.DynamicBroadcastConfig;

public class DocumentBroadcastConfig implements DynamicBroadcastConfig {
    
    private final DocumentNodeStore documentNodeStore;

    public DocumentBroadcastConfig(DocumentNodeStore documentNodeStore) {
        this.documentNodeStore = documentNodeStore;
    }

    @Override
    public String getConfig() {
        // currently not implemented
        return null;
    }

    @Override
    public List<Map<String, String>> getClientInfo() {
        ArrayList<Map<String, String>> list = new ArrayList<Map<String, String>>();
        for (ClusterNodeInfoDocument doc : ClusterNodeInfoDocument.all(documentNodeStore.getDocumentStore())) {
            if (!doc.isActive()) {
                continue;
            }
            Object broadcastId = doc.get(DynamicBroadcastConfig.ID);
            Object listener = doc.get(DynamicBroadcastConfig.LISTENER);
            if (broadcastId == null || listener == null) {
                // no id or no listener
                continue;
            }
            Map<String, String> map = new HashMap<String, String>();
            map.put(DynamicBroadcastConfig.ID, broadcastId.toString());
            map.put(DynamicBroadcastConfig.LISTENER, listener.toString());
            list.add(map);
        }
        return list;
    }

    @Override
    public String connect(Map<String, String> clientInfo) {
        ClusterNodeInfo info = documentNodeStore.getClusterInfo();
        info.setInfo(clientInfo);
        return "" + info.getId();
    }

    @Override
    public void disconnect(String id) {
        // ignore
    }

}
