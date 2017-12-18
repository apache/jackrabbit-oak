package org.apache.jackrabbit.oak.blob.cloud.s3;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;
import org.apache.jackrabbit.core.data.DataStore;
import org.apache.jackrabbit.oak.plugins.blob.datastore.AbstractDataStoreFactory;
import org.osgi.service.component.ComponentContext;

import java.util.Map;

@Component(policy = ConfigurationPolicy.REQUIRE,
        name = "org.apache.jackrabbit.oak.plugins.blob.datastore.S3DataStoreFactory",
        configurationFactory = true,
        label = "Apache Jackrabbit Oak S3DataStore Factory",
        description = "Factory allowing configuration of multiple instances of S3DataStores, " +
                "for use with CompositeDataStore.")
public class S3DataStoreFactory extends AbstractDataStoreFactory {
    @Override
    protected DataStore createDataStore(ComponentContext context, Map<String, Object> config) {
        DataStore ds = S3DataStoreService.createS3DataStore(context, config, getStatisticsProvider(), getDescription(), closer);
        return ds;
    }
}
