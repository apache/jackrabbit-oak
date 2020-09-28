package org.apache.jackrabbit.oak.explorer;

import com.google.common.io.Files;
import org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.spi.persistence.JournalFile;
import org.apache.jackrabbit.oak.segment.spi.persistence.SegmentNodeStorePersistence;

import java.io.IOException;

import static org.apache.jackrabbit.oak.segment.azure.tool.ToolUtils.newSegmentNodeStorePersistence;
import static org.apache.jackrabbit.oak.segment.file.FileStoreBuilder.fileStoreBuilder;

/**
 * Backend using a a remote Azure Segment Store.
 * <p>
 * The path must be in the form "{@code az:https://myaccount.blob.core.windows.net/container/repository}".
 * The secret key must be supplied as an environment variable {@codeAZURE_SECRET_KEY}
 */
public class AzureSegmentStoreExplorerBackend extends AbstractSegmentTarExplorerBackend {
    private final String path;
    private SegmentNodeStorePersistence persistence;

    public AzureSegmentStoreExplorerBackend(String path) {
        this.path = path;
    }

    @Override
    public void open() throws IOException {
        this.persistence = newSegmentNodeStorePersistence(ToolUtils.SegmentStoreType.AZURE, path);

        try {
            this.store = fileStoreBuilder(Files.createTempDir())
                    .withCustomPersistence(persistence)
                    .buildReadOnly();
        } catch (InvalidFileStoreVersionException e) {
            throw new IllegalStateException(e);
        }
        this.index = store.getTarReaderIndex();
    }

    protected JournalFile getJournal() {
        return persistence.getJournalFile();
    }
}
