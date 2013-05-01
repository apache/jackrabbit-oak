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

package org.apache.jackrabbit.oak.plugins.observation2;

import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.DATE;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.EVENTS_PATH;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.PATH;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.TYPE;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.USER_DATA;
import static org.apache.jackrabbit.oak.plugins.observation2.ObservationConstants.USER_ID;

import java.util.Collections;
import java.util.Iterator;

import javax.jcr.observation.Event;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Root;
import org.apache.jackrabbit.oak.api.Tree;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;

/**
 * TODO document
 */
public class EventQueueReader {
    private final Root root;
    private final NamePathMapper namePathMapper;

    private Tree bundles;
    private long nextBundleId = EventQueueWriterProvider.BUNDLE_ID.get();

    public EventQueueReader(Root root, NamePathMapper namePathMapper) {
        this.root = root;
        this.namePathMapper = namePathMapper;
    }

    public Iterator<Event> getEventBundle(final String id) {
        root.refresh();

        Iterator<Tree> events = getEvents(nextBundleId, id);
        if (events == null) {
            return null;
        }

        return Iterators.transform(events, new Function<Tree, Event>() {
            @Override
            public Event apply(Tree event) {
                return createEvent(event, id);
            }
        });
    }

    private Iterator<Tree> getEvents(long next, final String id) {
        if (bundles == null) {
            bundles = root.getTree(EVENTS_PATH);
        }

        Tree bundle = bundles.getChildOrNull(String.valueOf(next));
        if (bundle != null) {
            nextBundleId++;
            if (bundle.getChildrenCount() > 0) {
                return Iterators.filter(bundle.getChildren().iterator(),
                        new Predicate<Tree>() {
                    @Override
                    public boolean apply(Tree tree) {
                        return tree.hasChild(id);
                    }
                });
            }
        }
        return null;
    }

    private Event createEvent(Tree event, String id) {
        int type = (int) getLong(event, TYPE, 0);
        String path = getJcrPath(event);
        String userId = getString(event.getChildOrNull(id), USER_ID);
        long date = getLong(event, DATE, 0);
        String userData = getString(event.getChildOrNull(id), USER_DATA);
        return new EventImpl(type, path, userId, id, Collections.emptyMap(), date, userData);
    }

    private String getJcrPath(Tree event) {
        String path = getString(event, PATH);
        return path == null ? null : namePathMapper.getJcrPath(path);
    }

    private static long getLong(Tree event, String name, long defaultValue) {
        PropertyState p = event.getProperty(name);
        return p == null ? defaultValue : p.getValue(Type.LONG);
    }

    private static String getString(Tree event, String name) {
        PropertyState p = event.getProperty(name);
        return p == null ? null : p.getValue(Type.STRING);
    }
}
