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
 * Unless required by applicable law or agreed in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.jcr.observation;

import org.junit.Test;
import org.mockito.Mockito;
import org.apache.jackrabbit.oak.spi.whiteboard.CompositeRegistration;
import org.apache.jackrabbit.oak.api.ContentSession;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.apache.jackrabbit.commons.observation.ListenerTracker;
import org.apache.jackrabbit.oak.plugins.observation.filter.FilterProvider;
import org.apache.jackrabbit.oak.stats.StatisticManager;
import org.apache.jackrabbit.oak.plugins.observation.CommitRateLimiter;
import org.apache.jackrabbit.oak.api.blob.BlobAccessProvider;
import org.junit.Assert;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit cases for {@link ChangeProcessor} class
 */
public class ChangeProcessorTest {

    @Test
    public void testStopSetStoppedFlagCorrectly() {
        ChangeProcessor changeProcessor = createChangeProcessor();
        CompositeRegistration registration = Mockito.mock(CompositeRegistration.class);
        setRegistration(changeProcessor, registration);
        // Assert stopped flag is false before calling stop()
        Assert.assertFalse("stopped flag should be false before stop()", getStoppedFlag(changeProcessor).get());
        // Now call stop(), which should throw due to leave() without enter()
        changeProcessor.stop();
        Mockito.verify(registration).unregister();
        // Assert stopped flag is true
        Assert.assertTrue("stopped flag should be true after stop()", getStoppedFlag(changeProcessor).get());


    }

    @Test
    public void testToStringRunningStateBeforeAndAfterStop() {
        ChangeProcessor changeProcessor = createChangeProcessor();
        CompositeRegistration registration = Mockito.mock(CompositeRegistration.class);
        setRegistration(changeProcessor, registration);

        String toStringBeforeStop = changeProcessor.toString();
        System.out.println("Before stop: " + toStringBeforeStop);
        Assert.assertTrue("toString() before stop should indicate running=true: " + toStringBeforeStop,
                toStringBeforeStop.contains("running=true"));

        changeProcessor.stop();

        String toStringAfterStop = changeProcessor.toString();
        System.out.println("After stop: " + toStringAfterStop);
        Assert.assertTrue("toString() after stop should indicate running=false: " + toStringAfterStop,
                toStringAfterStop.contains("running=false"));
    }

    @Test
    public void testStopIdempotency() {
        ChangeProcessor changeProcessor = createChangeProcessor();
        CompositeRegistration registration = Mockito.mock(CompositeRegistration.class);
        setRegistration(changeProcessor, registration);
        // Assert stopped flag is false before calling stop()
        Assert.assertFalse("stopped flag should be false before stop()", getStoppedFlag(changeProcessor).get());
        // Call stop() first time
        changeProcessor.stop();
        Assert.assertTrue("stopped flag should be true after first stop()", getStoppedFlag(changeProcessor).get());
        Mockito.verify(registration).unregister();
        // Call stop() second time
        changeProcessor.stop();
        Assert.assertTrue("stopped flag should remain true after second stop()", getStoppedFlag(changeProcessor).get());
        // unregister should only be called once
        Mockito.verify(registration, Mockito.times(1)).unregister();
    }


    // helper methods

    private static ChangeProcessor createChangeProcessor() {
        ContentSession contentSession = Mockito.mock(ContentSession.class);
        NamePathMapper namePathMapper = Mockito.mock(NamePathMapper.class);
        ListenerTracker tracker = Mockito.mock(ListenerTracker.class);
        FilterProvider filter = Mockito.mock(FilterProvider.class);
        StatisticManager statisticManager = Mockito.mock(StatisticManager.class);
        int queueLength = 1;
        CommitRateLimiter commitRateLimiter = Mockito.mock(CommitRateLimiter.class);
        BlobAccessProvider blobAccessProvider = Mockito.mock(BlobAccessProvider.class);

        return new ChangeProcessor(
            contentSession,
            namePathMapper,
            tracker,
            filter,
            statisticManager,
            queueLength,
            commitRateLimiter,
            blobAccessProvider
        );
    }

    private static AtomicBoolean getStoppedFlag(ChangeProcessor changeProcessor) {
        try {
            Field stoppedField = ChangeProcessor.class.getDeclaredField("stopped");
            stoppedField.setAccessible(true);
            return (AtomicBoolean) stoppedField.get(changeProcessor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void setRegistration(ChangeProcessor changeProcessor, CompositeRegistration registration) {
        try {
            Field regField = ChangeProcessor.class.getDeclaredField("registration");
            regField.setAccessible(true);
            regField.set(changeProcessor, registration);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
