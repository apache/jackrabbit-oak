package org.apache.jackrabbit.oak.upgrade.cli;

import org.apache.jackrabbit.oak.segment.azure.AzureUtilities;
import org.apache.jackrabbit.oak.segment.azure.util.Environment;
import org.apache.jackrabbit.oak.upgrade.cli.container.NodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentAzureServicePrincipalNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.container.SegmentTarNodeStoreContainer;
import org.apache.jackrabbit.oak.upgrade.cli.parser.CliArgumentException;

import javax.jcr.RepositoryException;
import java.io.IOException;

import static org.junit.Assume.assumeNotNull;

public class SegmentTarToSegmentAzureServicePrincipalTest extends AbstractOak2OakTest {
    private static final Environment ENVIRONMENT = new Environment();
    private final NodeStoreContainer source;
    private final NodeStoreContainer destination;

    public SegmentTarToSegmentAzureServicePrincipalTest() throws IOException {
        source = new SegmentTarNodeStoreContainer();
        destination = new SegmentAzureServicePrincipalNodeStoreContainer();
    }

    @Override
    protected NodeStoreContainer getSourceContainer() {
        return source;
    }

    @Override
    protected NodeStoreContainer getDestinationContainer() {
        return destination;
    }

    @Override
    protected String[] getArgs() {
        return new String[]{source.getDescription(), destination.getDescription()};
    }

    @Override
    public void validateMigration() throws RepositoryException, IOException, CliArgumentException {
        assumeNotNull(ENVIRONMENT.getVariable(AzureUtilities.AZURE_ACCOUNT_NAME));
        assumeNotNull(ENVIRONMENT.getVariable(AzureUtilities.AZURE_TENANT_ID));
        assumeNotNull(ENVIRONMENT.getVariable(AzureUtilities.AZURE_CLIENT_ID));
        assumeNotNull(ENVIRONMENT.getVariable(AzureUtilities.AZURE_CLIENT_SECRET));

        super.validateMigration();
    }
}
