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
package org.apache.jackrabbit.oak.jcr.session;

import static org.apache.jackrabbit.oak.spi.toggle.Feature.newFeature;

import java.io.Closeable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.jackrabbit.guava.common.base.Strings;
import org.apache.jackrabbit.oak.api.jmx.RepositoryManagementMBean;
import org.apache.jackrabbit.oak.spi.toggle.Feature;
import org.apache.jackrabbit.oak.spi.whiteboard.Whiteboard;
import org.apache.jackrabbit.oak.spi.whiteboard.WhiteboardUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A delay mechanism for Session.save() operations. By default, Session.save
 * calls are not delayed. If enabled, some of the save() operations can be
 * delayed for a certain number of microseconds.
 *
 * This facility is enabled / disabled via feature toggle, and controlled via
 * JMX bean, or (for testing) via two system properties. There is no attempt to
 * control the delay, or which threads to delay, from within. It is meant for
 * emergency situation, specially for cases where some threads write too much.
 */
public class SessionSaveDelayer implements Closeable {

    private static final Logger LOG = LoggerFactory.getLogger(SessionSaveDelayer.class);

    private static final String FT_SAVE_DELAY_NAME = "FT_SAVE_DELAY_OAK-11766";
    private static final String ENABLED_PROP_NAME = "oak.sessionSaveDelayer";
    private static final String CONFIG_PROP_NAME = "oak.sessionSaveDelayerConfig";

    private final boolean enabledViaSysPropertey = Boolean.getBoolean(ENABLED_PROP_NAME);
    private final String sysPropertyConfig = System.getProperty(CONFIG_PROP_NAME, "");
    private final Feature feature;
    private final Whiteboard whiteboard;
    private final AtomicBoolean closed = new AtomicBoolean();

    private RepositoryManagementMBean cachedMbean;
    private String lastConfigJson;
    private SessionSaveDelayerConfig lastConfig;
    private volatile boolean logNextDelay;

    public SessionSaveDelayer(@NotNull Whiteboard whiteboard) {
        this.feature = newFeature(FT_SAVE_DELAY_NAME, whiteboard);
        LOG.info("Initialized");
        if (enabledViaSysPropertey) {
            LOG.info("Enabled via system property: " + ENABLED_PROP_NAME);
        }
        this.whiteboard = whiteboard;
    }

    private RepositoryManagementMBean getRepositoryMBean() {
        if (cachedMbean == null) {
            cachedMbean = WhiteboardUtils.getService(whiteboard, RepositoryManagementMBean.class);
        }
        return cachedMbean;
    }

    public long delayIfNeeded(String userData) {
        if (closed.get() || (!feature.isEnabled() && !enabledViaSysPropertey)) {
            return 0;
        }
        String config = sysPropertyConfig;
        RepositoryManagementMBean mbean = getRepositoryMBean();
        if (mbean != null) {
            String jmxConfig = mbean.getSessionSaveDelayerConfig();
            if (!Strings.isNullOrEmpty(jmxConfig)) {
                config = jmxConfig;
            }
        }
        if (Strings.isNullOrEmpty(config)) {
            return 0;
        }
        if (!config.equals(lastConfigJson)) {
            logNextDelay = true;
            lastConfigJson = config;
            try {
                // reset, if already set
                lastConfig = null;
                lastConfig = SessionSaveDelayerConfig.fromJson(config);
                LOG.info("New config: {}", lastConfig.toString());
            } catch (IllegalArgumentException e) {
                LOG.warn("Can not parse config {}", e);
                // don't delay
                return 0;
            }
        }
        if (lastConfig == null) {
            return 0;
        }
        String threadName = Thread.currentThread().getName();
        long delayNanos = lastConfig.getDelayNanos(threadName, userData, null);
        if (delayNanos > 0) {
            long millis = delayNanos / 1_000_000;
            int nanos = (int) (delayNanos % 1_000_000);
            if (logNextDelay) {
                LOG.info("Sleep {} ms {} ns for user {}", millis, nanos, userData);
                logNextDelay = false;
            }
            try {
                Thread.sleep(millis, nanos);
            } catch (InterruptedException e) {
                // ignore
                Thread.currentThread().interrupt();
            }
        }
        return delayNanos;
    }

    @Override
    public void close() {
        if (!closed.getAndSet(true)) {
            feature.close();
        }
    }

}
