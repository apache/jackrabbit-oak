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
package org.apache.jackrabbit.oak.jcr.observation;

import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

import javax.jcr.RepositoryException;
import javax.jcr.UnsupportedRepositoryOperationException;
import javax.jcr.observation.EventJournal;
import javax.jcr.observation.EventListener;
import javax.jcr.observation.EventListenerIterator;
import javax.jcr.observation.ObservationManager;

import org.apache.jackrabbit.commons.iterator.EventListenerIteratorAdapter;
import org.apache.jackrabbit.oak.api.ChangeExtractor;
import org.apache.jackrabbit.oak.jcr.SessionDelegate;
import org.apache.jackrabbit.oak.jcr.util.LazyValue;

public class ObservationManagerImpl implements ObservationManager {
    private final SessionDelegate sessionDelegate;
    private final Map<EventListener, ChangeProcessor> processors =
            new HashMap<EventListener, ChangeProcessor>();

    private final LazyValue<Timer> timer;

    public ObservationManagerImpl(SessionDelegate sessionDelegate, LazyValue<Timer> timer) {
        this.sessionDelegate = sessionDelegate;
        this.timer = timer;
    }

    public void dispose() {
        for (ChangeProcessor processor : processors.values()) {
            processor.stop();
        }
    }

    @Override
    public void addEventListener(EventListener listener, int eventTypes, String absPath,
            boolean isDeep, String[] uuid, String[] nodeTypeName, boolean noLocal)
            throws RepositoryException {

        // TODO: support noLocal flag!?
        ChangeProcessor processor = processors.get(listener);
        if (processor == null) {
            ChangeExtractor extractor = sessionDelegate.getChangeExtractor();
            ChangeFilter filter = new ChangeFilter(eventTypes, absPath, isDeep, uuid, nodeTypeName, noLocal);
            ChangeProcessor changeProcessor = new ChangeProcessor(sessionDelegate.getNamePathMapper(), extractor,
                    listener, filter);
            processors.put(listener, changeProcessor);
            timer.get().schedule(changeProcessor, 0, 1000);
        }
        else {
            ChangeFilter filter = new ChangeFilter(eventTypes, absPath, isDeep, uuid, nodeTypeName, noLocal);
            processor.setFilter(filter);
        }
    }

    @Override
    public void removeEventListener(EventListener listener) throws RepositoryException {
        ChangeProcessor processor = processors.remove(listener);
        if (processor != null) {
            processor.stop();
        }
    }

    @Override
    public EventListenerIterator getRegisteredEventListeners() throws RepositoryException {
        return new EventListenerIteratorAdapter(processors.keySet());
    }

    @Override
    public void setUserData(String userData) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException("User data not supported");
    }

    @Override
    public EventJournal getEventJournal() throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

    @Override
    public EventJournal getEventJournal(int eventTypes, String absPath, boolean isDeep, String[] uuid, String[]
            nodeTypeName) throws RepositoryException {
        throw new UnsupportedRepositoryOperationException();
    }

}
