package org.apache.jackrabbit.oak.osgi;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.newHashMap;

import java.io.Closeable;
import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.io.Closeables;
import org.apache.jackrabbit.oak.spi.commit.Observable;
import org.apache.jackrabbit.oak.spi.commit.Observer;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

public class ObserverTracker implements ServiceTrackerCustomizer {
    private final Observable observable;
    private final Map<ServiceReference, Closeable> subscriptions = newHashMap();

    private BundleContext bundleContext;
    private ServiceTracker tracker;

    public ObserverTracker(@Nonnull Observable observable) {
        this.observable = checkNotNull(observable);
    }

    public void start(@Nonnull BundleContext bundleContext) {
        checkState(this.bundleContext == null);
        this.bundleContext = checkNotNull(bundleContext);
        tracker = new ServiceTracker(bundleContext, Observer.class.getName(), this);
        tracker.open();
    }

    public void stop() {
        checkState(this.bundleContext != null);
        tracker.close();
    }

    //------------------------< ServiceTrackerCustomizer >----------------------

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = bundleContext.getService(reference);

        if (service instanceof Observer) {
            Closeable subscription = observable.addObserver((Observer) service);
            subscriptions.put(reference, subscription);
            return service;
        } else {
            bundleContext.ungetService(reference);
            return null;
        }
    }

    @Override
    public void modifiedService(ServiceReference reference, Object service) {
        // nothing to do
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        Closeables.closeQuietly(subscriptions.get(reference));
        bundleContext.ungetService(reference);
    }

}
