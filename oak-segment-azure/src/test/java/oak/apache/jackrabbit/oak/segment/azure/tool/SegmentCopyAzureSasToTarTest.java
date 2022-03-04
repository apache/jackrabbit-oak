package oak.apache.jackrabbit.oak.segment.azure.tool;

import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;


public class SegmentCopyAzureSasToTarTest extends SegmentCopyTestBase{
    @Override
    protected SegmentNodeStorePersistence getSrcPersistence() throws Exception {
        return getAzurePersistence();
    }

    @Override
    protected SegmentNodeStorePersistence getDestPersistence() {
        return getTarPersistence();
    }

    @Override
    protected String getSrcPathOrUri(){
        return getAzurePersistencePathOrUriSas();
    }

    @Override
    protected String getDestPathOrUri() {
        return getTarPersistencePathOrUri();
    }
}
