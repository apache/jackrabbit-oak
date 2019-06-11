package org.apache.jackrabbit.oak.segment.spi.monitor;

import java.util.concurrent.TimeUnit;

public class RemoteStoreMonitorAdapter implements RemoteStoreMonitor {


    @Override
    public void requestCount() {
        // Intentionally left blank
    }

    @Override
    public void requestError() {
        // Intentionally left blank
    }

    @Override
    public void requestDuration(long duration, TimeUnit timeUnit) {
        // Intentionally left blank
    }
}
