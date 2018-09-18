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

/**
 * A LeaseFailureHandler can be provided to the DocumentMK.Builder 
 * and will be passed on to the ClusterNodeInfo for use upon
 * lease failure.
 * <p>
 * When ClusterNodeInfo does not have such a LeaseFailureHandler, 
 * the only thing it does is fail every subsequent access with
 * an exception - but it doesn't do fancy things like stopping
 * the oak-store-document bundle etc. Such an operation must be provided
 * in a LeaseFailureHandler.
 */
public interface LeaseFailureHandler {

    /**
     * Invoked by ClusterNodeInfo when it detects a lease
     * failure and has started preventing any further access
     * to the DocumentStore by throwing exceptions - what's
     * now left is any further actions that should be taken
     * such as eg stopping the oak-store-document bundle. This part
     * however is optional from the ClusterNodeInfo's pov
     * and must be done by here in this LeaseFailureHandler.
     */
    public void handleLeaseFailure();
    
}
