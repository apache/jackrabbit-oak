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
package org.apache.jackrabbit.oak.plugins.document.persistentCache.broadcast;

import java.util.List;
import java.util.Map;

/**
 * Broadcast configuration. Configuration is dynamic, that means can change over
 * time. The configuration consists of a list of connected clients. Each client
 * can connect and disconnect, and therefore allow other clients to connect to
 * it.
 */
public interface DynamicBroadcastConfig {
    
    /**
     * The unique id of this client.
     */
    String ID = "broadcastId";
    
    /**
     * The listener address, for example the IP address and port.
     */
    String LISTENER = "broadcastListener";
    
    /**
     * Get the global configuration data that is not associated to a specific client.
     * 
     * @return the configuration
     */
    String getConfig();
    
    /**
     * Get the client info of all connected clients.
     * 
     * @return the list of client info maps
     */
    List<Map<String, String>> getClientInfo();
    
    /**
     * Announce a new client to others.
     * 
     * @param clientInfo the client info
     * @return a unique id (to be used when disconnecting)
     */
    String connect(Map<String, String> clientInfo);
    
    /**
     * Sign off.
     * 
     * @param id the unique id
     */
    void disconnect(String id);

}
