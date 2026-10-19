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
package org.apache.jackrabbit.oak.plugins.index.luceneNg;

import org.apache.jackrabbit.oak.spi.commit.BackgroundObserver;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class LuceneNgIndexProviderServiceTest {

    @TempDir
    public Path tempDir;

    @Test
    public void copyOnReadEnabled_createsCopierAndPassesToTracker() throws Exception {
        LuceneNgIndexProviderService service = new LuceneNgIndexProviderService();
        BundleContext bundleContext = mock(BundleContext.class);
        when(bundleContext.getProperty("repository.home")).thenReturn(tempDir.toFile().getAbsolutePath());
        when(bundleContext.registerService(anyString(), any(), any()))
                .thenReturn(mock(ServiceRegistration.class));

        LuceneNgIndexProviderService.Config config = mock(LuceneNgIndexProviderService.Config.class);
        when(config.disabled()).thenReturn(false);
        when(config.enableCopyOnReadSupport()).thenReturn(true);
        when(config.localIndexDir()).thenReturn("");
        when(config.prefetchIndexFiles()).thenReturn(false);

        activate(service, bundleContext, config); // reflection helper - see below

        File expectedIndexDir = new File(tempDir.toFile(), "index");
        assertTrue(expectedIndexDir.isDirectory(), "expected index dir to be created under repository.home/index");

        deactivate(service); // reflection helper - see below
    }

    @Test
    public void copyOnReadDisabled_doesNotCreateCopier() throws Exception {
        LuceneNgIndexProviderService service = new LuceneNgIndexProviderService();
        BundleContext bundleContext = mock(BundleContext.class);
        when(bundleContext.registerService(anyString(), any(), any()))
                .thenReturn(mock(ServiceRegistration.class));

        LuceneNgIndexProviderService.Config config = mock(LuceneNgIndexProviderService.Config.class);
        when(config.disabled()).thenReturn(false);
        when(config.enableCopyOnReadSupport()).thenReturn(false);

        activate(service, bundleContext, config);

        Field copierField = LuceneNgIndexProviderService.class.getDeclaredField("indexCopier");
        copierField.setAccessible(true);
        assertNull(copierField.get(service));
        verify(bundleContext).registerService(eq(Observer.class.getName()), isA(BackgroundObserver.class), isNull());

        deactivate(service);
    }

    @Test
    public void readerAndWriterCachingCanBeEnabledIndependently() throws Exception {
        for (boolean readerEnabled : new boolean[] {false, true}) {
            LuceneNgIndexProviderService service = new LuceneNgIndexProviderService();
            BundleContext context = mock(BundleContext.class);
            when(context.getProperty("repository.home")).thenReturn(Files.createTempDirectory(tempDir, "folder").toFile().getAbsolutePath());
            when(context.registerService(anyString(), any(), any())).thenReturn(mock(ServiceRegistration.class));
            LuceneNgIndexProviderService.Config config = mock(LuceneNgIndexProviderService.Config.class);
            when(config.enableCopyOnReadSupport()).thenReturn(readerEnabled);
            when(config.enableReadBeforeWriteSupport()).thenReturn(!readerEnabled);
            activate(service, context, config);
            try {
                assertNotNull(field(service, "indexCopier"));
                Object trackerCopier = field(field(service, "indexTracker"), "copier");
                Object writerCopier = field(field(service, "editorProvider"), "copier");
                if (readerEnabled) {
                    assertNotNull(trackerCopier);
                    assertNull(writerCopier);
                } else {
                    assertNull(trackerCopier);
                    assertNotNull(writerCopier);
                }
            } finally {
                deactivate(service);
            }
        }
    }

    private static Object field(Object object, String name) throws Exception {
        Field field = object.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(object);
    }

    private static void activate(LuceneNgIndexProviderService service, BundleContext ctx,
                                 LuceneNgIndexProviderService.Config config) throws Exception {
        Method m = LuceneNgIndexProviderService.class.getDeclaredMethod("activate", BundleContext.class, LuceneNgIndexProviderService.Config.class);
        m.setAccessible(true);
        m.invoke(service, ctx, config);
    }

    private static void deactivate(LuceneNgIndexProviderService service) throws Exception {
        Method m = LuceneNgIndexProviderService.class.getDeclaredMethod("deactivate");
        m.setAccessible(true);
        m.invoke(service);
    }
}
