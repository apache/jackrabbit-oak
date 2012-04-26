package org.apache.jackrabbit.oak.osgi;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.jackrabbit.mk.api.MicroKernel;
import org.apache.jackrabbit.oak.query.index.QueryIndex;
import org.apache.jackrabbit.oak.query.index.QueryIndexProvider;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.ServiceTracker;
import org.osgi.util.tracker.ServiceTrackerCustomizer;

public class OsgiIndexProvider implements ServiceTrackerCustomizer, QueryIndexProvider {

    private BundleContext context;

    private ServiceTracker tracker;

    private final Map<ServiceReference, QueryIndexProvider> providers =
        new HashMap<ServiceReference, QueryIndexProvider>();

    public void start(BundleContext bundleContext) throws Exception {
        context = bundleContext;
        tracker = new ServiceTracker(
                bundleContext, QueryIndexProvider.class.getName(), this);
        tracker.open();
    }

    public void stop() throws Exception {
        tracker.close();
    }

    @Override
    public Object addingService(ServiceReference reference) {
        Object service = context.getService(reference);
        if (service instanceof QueryIndexProvider) {
            QueryIndexProvider provider = (QueryIndexProvider) service;
            providers.put(reference, provider);
            return service;
        } else {
            context.ungetService(reference);
            return null;
        }
    }

    @Override
    public void modifiedService(ServiceReference reference, Object service) {
        // nothing to do
    }

    @Override
    public void removedService(ServiceReference reference, Object service) {
        providers.remove(reference);
        context.ungetService(reference);
    }

    @Override
    public void init() {
        // nothing to do
    }

    @Override
    public List<QueryIndex> getQueryIndexes(MicroKernel mk) {
        if (providers.size() == 0) {
            return Collections.emptyList();
        } else if (providers.size() == 1) {
            return providers.get(0).getQueryIndexes(mk);
        } else {
            // TODO combine indexes
            return null;
        }
    }

}
