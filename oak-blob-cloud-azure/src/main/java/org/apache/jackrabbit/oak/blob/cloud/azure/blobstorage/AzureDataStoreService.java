package org.apache.jackrabbit.oak.blob.cloud.azure.blobstorage;

import org.apache.felix.scr.annotations.Component;
import org.apache.felix.scr.annotations.ConfigurationPolicy;

@Component(policy = ConfigurationPolicy.REQUIRE, name = AzureDataStoreService.NAME, metatype = true)
public class AzureDataStoreService extends AbstractAzureDataStoreService {
    public static final String NAME = "org.apache.jackrabbit.oak.plugins.blob.datastore.AzureDataStore";
}
