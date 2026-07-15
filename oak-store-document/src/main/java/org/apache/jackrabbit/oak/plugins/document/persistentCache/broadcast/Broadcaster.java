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

import java.nio.ByteBuffer;

/**
 * A broadcast mechanism that is able to send and receive commands.
 */
public interface Broadcaster {
    
    /**
     * Change the dynamic broadcasting configuration.
     * 
     * @param broadcastConfig the new configuration
     */
    void setBroadcastConfig(DynamicBroadcastConfig broadcastConfig);
    
    /**
     * Send a message.
     * 
     * @param buff the buffer
     */
    void send(ByteBuffer buff);
    
    /**
     * Add a listener for new messages.
     * 
     * @param listener the listener
     */
    void addListener(Listener listener);
    
    /**
     * Remove a listener.
     * 
     * @param listener the listener
     */
    void removeListener(Listener listener);
    
    /**
     * Close the broadcaster.
     */
    void close();

    /**
     * A listener for new messages.
     */
    public interface Listener {
        
        /**
         * Receive a message.
         * 
         * @param buff the buffer
         */
        void receive(ByteBuffer buff);
        
    }
    
}
