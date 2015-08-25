package org.apache.jackrabbit.oak.upgrade;

import org.apache.jackrabbit.oak.Oak;
import org.apache.jackrabbit.oak.jcr.Jcr;
import org.apache.jackrabbit.oak.jcr.repository.RepositoryImpl;
import org.apache.jackrabbit.oak.plugins.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.plugins.segment.file.FileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.junit.Before;

import javax.jcr.RepositoryException;
import javax.jcr.Session;

import java.io.File;
import java.io.IOException;

public class IncludeExcludeSidegradeTest extends IncludeExcludeUpgradeTest {

    @Before
    public synchronized void upgradeRepository() throws Exception {
        if (targetNodeStore == null) {
            File directory = getTestDirectory();
            File source = new File(directory, "source");
            source.mkdirs();
            FileStore fileStore = FileStore.newFileStore(source).create();
            SegmentNodeStore segmentNodeStore = SegmentNodeStore.newSegmentNodeStore(fileStore).create();
            RepositoryImpl repository = (RepositoryImpl) new Jcr(new Oak(segmentNodeStore)).createRepository();
            Session session = repository.login(CREDENTIALS);
            try {
                createSourceContent(session);
            } finally {
                session.save();
                session.logout();
                repository.shutdown();
                fileStore.close();
            }
            final NodeStore target = getTargetNodeStore();
            doUpgradeRepository(source, target);
            targetNodeStore = target;
        }
    }

    @Override
    protected void doUpgradeRepository(File source, NodeStore target) throws RepositoryException, IOException {
        FileStore fileStore = FileStore.newFileStore(source).create();
        SegmentNodeStore segmentNodeStore = SegmentNodeStore.newSegmentNodeStore(fileStore).create();
        try {
            final RepositorySidegrade sidegrade = new RepositorySidegrade(segmentNodeStore, target);
            sidegrade.setIncludes(
                    "/content/foo/en",
                    "/content/assets/foo"
            );
            sidegrade.setExcludes(
                    "/content/assets/foo/2013",
                    "/content/assets/foo/2012",
                    "/content/assets/foo/2011",
                    "/content/assets/foo/2010"
            );
            sidegrade.copy();
        } finally {
            fileStore.close();
        }
    }
}
