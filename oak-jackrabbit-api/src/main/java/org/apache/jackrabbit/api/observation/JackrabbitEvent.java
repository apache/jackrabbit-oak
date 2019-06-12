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
package org.apache.jackrabbit.api.observation;

import javax.jcr.observation.Event;

/**
 * This is an extension of the event interface which provides
 * a method to detect whether the changes happened on locally
 * or remotely in a clustered environment.
 */
public interface JackrabbitEvent extends Event {

    /**
     * Return a flag indicating whether this is an externally generated event.
     *
     * @return <code>true</code> if this is an external event;
     *         <code>false</code> otherwise
     */
    boolean isExternal();
}
