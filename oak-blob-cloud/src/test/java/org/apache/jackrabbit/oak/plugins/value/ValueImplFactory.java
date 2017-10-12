package org.apache.jackrabbit.oak.plugins.value;

import org.apache.jackrabbit.oak.api.Blob;
import org.apache.jackrabbit.oak.api.PropertyState;
import org.apache.jackrabbit.oak.api.Type;
import org.apache.jackrabbit.oak.namepath.NamePathMapper;
import org.mockito.Mockito;

import javax.annotation.Nonnull;
import javax.jcr.RepositoryException;

/**
 * Created by boston on 25/09/2017.
 */
public class ValueImplFactory {
    public static ValueImpl newValue(final Blob blob, @Nonnull PropertyState property, int index, @Nonnull NamePathMapper namePathMapper) throws RepositoryException {
        Mockito.when(property.count()).thenReturn(1);
        return new ValueImpl(property, index,namePathMapper) {
            @Override
            public Blob getBlob() {
                return blob;
            }
        };
    }
}
