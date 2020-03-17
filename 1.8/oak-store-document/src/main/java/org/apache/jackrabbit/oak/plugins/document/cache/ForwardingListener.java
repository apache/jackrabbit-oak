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

import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;

/**
 * Listener which forwards the notifications to a delegate. It is used to bridge
 * multiple instances.
 *
 */
public class ForwardingListener<K, V>
        implements RemovalListener<K, V> {
    private RemovalListener<K, V> delegate;

    public ForwardingListener() {
    }

    public ForwardingListener(RemovalListener<K, V> delegate) {
        this.delegate = delegate;
    }

    @Override
    public void onRemoval(RemovalNotification<K, V> notification) {
        if (delegate != null) {
            delegate.onRemoval(notification);
        }
    }

    public void setDelegate(RemovalListener<K, V> delegate) {
        this.delegate = delegate;
    }

    public static <K, V> ForwardingListener<K, V> newInstance() {
        return new ForwardingListener<K, V>();
    }

    public static <K, V> ForwardingListener<K, V> newInstance(RemovalListener<K, V> delegate) {
        return new ForwardingListener<K, V>(delegate);
    }
}