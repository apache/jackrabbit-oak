package org.apache.jackrabbit.oak.api.conversion;

import org.osgi.annotation.versioning.ProviderType;

import javax.jcr.Value;
import java.net.URI;

/**
 * Created by boston on 07/09/2017.
 */
@ProviderType
public interface URIProvider {

    URI toURI(Value value);
}
