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
package org.apache.jackrabbit.api.jmx;

import javax.management.openmbean.CompositeData;

/**
 * MBean interface for exposing information about a registered observation
 * listener.
 *
 * @see <a href="https://issues.apache.org/jira/browse/JCR-3608">JCR-3608</a>
 */
public interface EventListenerMBean {

    /** Class name of the event listener */
    String getClassName();
    
    /** toString of the event listener */
    String getToString();

    /** Stack trace of where the listener was registered */
    String getInitStackTrace();

    /** Event types of the listener registration */
    int getEventTypes();

    /** Absolute path of the listener registration */
    String getAbsPath();

    /** Whether the listener registration is deep */
    boolean isDeep();

    /** UUIDs of the listener registration */
    String[] getUuid();

    /** Node types of the listener registration */
    String[] getNodeTypeName();

    /** Whether the listener registration is non-local */
    boolean isNoLocal();

    /** Number of {@code onEvent()} calls made on the listener */
    long getEventDeliveries();

    /** Average number of {@code onEvent()} calls per hour */
    long getEventDeliveriesPerHour();

    /** Average time (in microseconds) taken per {@code onEvent()} call */
    long getMicrosecondsPerEventDelivery();

    /** Number of individual events delivered to the listener */
    long getEventsDelivered();

    /** Average number of individual events delivered per hour */
    long getEventsDeliveredPerHour();

    /** Average time (in microseconds) taken per event delivered */
    long getMicrosecondsPerEventDelivered();

    /** Ratio of time spent in event processing */
    double getRatioOfTimeSpentProcessingEvents();

    /** Ratio of time spent in event listener vs. the overall event processing */
    double getEventConsumerTimeRatio();

    /** Is user information accessed without checking if an event is external? */
    boolean isUserInfoAccessedWithoutExternalsCheck();

    /** Is user information accessed from an external event? */
    boolean isUserInfoAccessedFromExternalEvent();

    /** Is date information accessed without checking if an event is external? */
    boolean isDateAccessedWithoutExternalsCheck();

    /** Is date information accessed from an external event? */
    boolean isDateAccessedFromExternalEvent();

    /**
     * The time difference between the current system time and the head (oldest)
     * element in the queue in milliseconds. This method returns zero if the
     * queue is empty.
     */
    long getQueueBacklogMillis();

    /**
     * {@link org.apache.jackrabbit.api.stats.TimeSeries time series} of the number of
     * items related to generating observation events that are currently queued by the
     * system. The exact nature of these items is implementation specific and might not
     * be in a one to one relation with the number of pending JCR events.
     * @return  time series of the queue length
     */
    CompositeData getQueueLength();

    /**
     * @return  time series of the number of JCR events
     */
    CompositeData getEventCount();

    /**
     * @return  time series of the time it took an event listener to process JCR events.
     */
    CompositeData getEventConsumerTime();

    /**
     * @return  time series of the time it took the system to produce JCR events.
     */
    CompositeData getEventProducerTime();

}
