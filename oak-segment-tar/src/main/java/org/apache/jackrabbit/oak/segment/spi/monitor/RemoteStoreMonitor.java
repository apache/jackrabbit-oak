package org.apache.jackrabbit.oak.segment.spi.monitor;

import java.util.concurrent.TimeUnit;

public interface RemoteStoreMonitor {

    public void requestCount();

    public void requestError();

    public void requestDuration(long duration, TimeUnit timeUnit);

}
