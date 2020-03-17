/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jackrabbit.oak.jcr.observation.filter;

import org.apache.jackrabbit.api.observation.JackrabbitEventFilter;
import org.apache.jackrabbit.oak.jcr.observation.OakEventFilterImpl;
import org.apache.jackrabbit.oak.jcr.observation.ObservationManagerImpl;

/**
 * Static factory that allows wrapping a JackrabbitEventFilter into an
 * OakEventFilter that contains some oak specific extensions.
 * <p>
 * The resulting filter can subsequently be used in
 * ObservationManagerImpl.addEventListener as usual.
 * 
 * @see ObservationManagerImpl#addEventListener(javax.jcr.observation.EventListener,
 *      JackrabbitEventFilter)
 */
public class FilterFactory {

    /**
     * Wrap a JackrabbitEventFilter into its corresponding oak extension,
     * OakEventFilter, on which some Oak specific observation filter extensions
     * can then be used.
     * 
     * @param baseFilter
     *            the base filter which contains other properties. Changes to
     *            the resulting oak filter "write-through" to the underlying
     *            baseFilter (for the features covered by the underlying) and
     *            similarly changes to the baseFilter are seen by the resulting
     *            oak filter. Note that this "write-through" behavior does no 
     *            longer apply after a listener was registered, ie changing
     *            a filter after registration doesn't alter it for that listener.
     * @return an OakEventFilter upon which Oak specific observation filtering
     *         extensions can be activated and then used when adding an
     *         EventListener with the ObservationManagerImpl.
     */
    public static OakEventFilter wrap(JackrabbitEventFilter baseFilter) {
        return new OakEventFilterImpl(baseFilter);
    }
}
