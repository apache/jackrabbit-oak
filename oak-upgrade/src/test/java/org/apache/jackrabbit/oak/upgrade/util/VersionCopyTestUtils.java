package org.apache.jackrabbit.oak.upgrade.util;

import static org.apache.jackrabbit.oak.plugins.version.VersionConstants.MIX_REP_VERSIONABLE_PATHS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import javax.jcr.Node;
import javax.jcr.Property;
import javax.jcr.PropertyType;
import javax.jcr.RepositoryException;
import javax.jcr.Session;
import javax.jcr.version.Version;
import javax.jcr.version.VersionHistory;
import javax.jcr.version.VersionManager;

import org.apache.jackrabbit.JcrConstants;
import org.apache.jackrabbit.commons.JcrUtils;
import org.apache.jackrabbit.oak.upgrade.RepositoryUpgrade;

public class VersionCopyTestUtils {

    public static String createVersionableNode(Session session, String versionablePath)
            throws RepositoryException, InterruptedException {
        final VersionManager versionManager = session.getWorkspace().getVersionManager();
        final Node versionable = JcrUtils.getOrCreateUniqueByPath(session.getRootNode(), versionablePath,
                JcrConstants.NT_UNSTRUCTURED);
        versionable.addMixin("mix:versionable");
        versionable.setProperty("version", "root");
        session.save();

        final String path = versionable.getPath();
        final List<String> versionNames = new ArrayList<String>();
        for (int i = 0; i < 3; i++) {
            versionable.setProperty("version", "1." + i);
            session.save();
            final Version v = versionManager.checkpoint(path);
            versionNames.add(v.getName());
        }

        final VersionHistory history = versionManager.getVersionHistory(path);
        for (final String versionName : versionNames) {
            history.addVersionLabel(versionName, String.format("version %s", versionName), false);
        }
        return history.getPath();
    }

    public static boolean isVersionable(Session session, String path) throws RepositoryException {
        return session.getNode(path).isNodeType(JcrConstants.MIX_VERSIONABLE);
    }

    public interface RepositoryUpgradeSetup {
        void setup(RepositoryUpgrade upgrade);
    }
}
