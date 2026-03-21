/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.jackrabbit.oak.segment.test;

import org.apache.commons.io.file.PathUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.jackrabbit.oak.segment.SegmentNodeStore;
import org.apache.jackrabbit.oak.segment.SegmentNodeStoreBuilders;
import org.apache.jackrabbit.oak.segment.file.AbstractFileStore;
import org.apache.jackrabbit.oak.segment.file.FileStore;
import org.apache.jackrabbit.oak.segment.file.FileStoreBuilder;
import org.apache.jackrabbit.oak.segment.file.InvalidFileStoreVersionException;
import org.apache.jackrabbit.oak.segment.file.ReadOnlyFileStore;
import org.apache.jackrabbit.oak.spi.state.NodeStore;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@link ParameterResolver} implementation that supports injecting {@link FileStore}, {@link ReadOnlyFileStore},
 * {@link NodeStore} and {@link SegmentNodeStore} instances into test methods. The resolver will create a new
 * {@code FileStore} or {@code ReadOnlyFileStore} instance, and a {@code SegmentNodeStore} instance backed by it.
 * <p>
 * If a {@code FileStore} and a {@code ReadOnlyFileStore} parameter are declared in the same test method, a
 * {@code NodeStore} parameter will be backed by the one that appears first in the parameter list.
 */
public class FileStoreParameterResolver implements ParameterResolver {

    private static final Namespace NAMESPACE = Namespace.create(FileStoreParameterResolver.class);

    private static final Set<Class<?>> SUPPORTED_TYPES = Set.of(
            FileStore.class,
            ReadOnlyFileStore.class,
            NodeStore.class,
            SegmentNodeStore.class
    );

    private final Consumer<FileStoreBuilder> builderCallback;

    public FileStoreParameterResolver(Consumer<FileStoreBuilder> builderCallback) {
        this.builderCallback = builderCallback;
    }

    @Override
    public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) {
        return SUPPORTED_TYPES.contains(parameterContext.getParameter().getType());
    }

    @Override
    @SuppressWarnings("unchecked")
    public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
        Class<?> type = parameterContext.getParameter().getType();
        if (AbstractFileStore.class.isAssignableFrom(type)) {
            return getOrCreateFileStore(extensionContext, (Class<? extends AbstractFileStore>) type);
        }

        if (type == NodeStore.class ||type == SegmentNodeStore.class) {
            ExtensionContext.Store store = extensionContext.getStore(NAMESPACE);
            Executable declaringExecutable = parameterContext.getDeclaringExecutable();
            final Class<? extends AbstractFileStore> fileStoreClass = Stream.of(declaringExecutable.getParameters())
                    .map(Parameter::getType)
                    .filter(AbstractFileStore.class::isAssignableFrom)
                    .map(c -> (Class<? extends AbstractFileStore>) c)
                    .findFirst()
                    .orElse(null);
            return store.getOrComputeIfAbsent(type.getName(), k -> {
                if (fileStoreClass == ReadOnlyFileStore.class) {
                    ReadOnlyFileStore fileStore = getOrCreateFileStore(extensionContext, ReadOnlyFileStore.class);
                    return SegmentNodeStoreBuilders.builder(fileStore).build();
                } else {
                    FileStore fileStore = getOrCreateFileStore(extensionContext, FileStore.class);
                    return SegmentNodeStoreBuilders.builder(fileStore).build();
                }
            }, SegmentNodeStore.class);
        }

        throw new ParameterResolutionException("Unsupported type " + type);
    }

    @SuppressWarnings("unchecked")
    private <T extends AbstractFileStore> T getOrCreateFileStore(ExtensionContext ctx, Class<T> type) {
        ExtensionContext.Store store = ctx.getStore(NAMESPACE);
        CloseablePath segmentstoreDir = store.getOrComputeIfAbsent("tempdir-for-" + FileStore.class.getSimpleName(),
                key -> new CloseablePath(computePathForTest(ctx)),
                CloseablePath.class
        );
        return store.getOrComputeIfAbsent(type.getName(), k -> {
            try {
                Files.createDirectories(segmentstoreDir.path);
                FileStoreBuilder fileStoreBuilder = FileStoreBuilder.fileStoreBuilder(segmentstoreDir.path.toFile())
                        .withStringCacheSize(0)
                        .withTemplateCacheSize(0)
                        .withSegmentCacheSize(0);
                builderCallback.accept(fileStoreBuilder);
                if (type == ReadOnlyFileStore.class) {
                    return (T) fileStoreBuilder.buildReadOnly();
                } else if (type == FileStore.class) {
                    return (T) fileStoreBuilder.build();
                }  else {
                    throw new IllegalArgumentException("Unsupported type " + type);
                }
            } catch (InvalidFileStoreVersionException | IOException e) {
                throw new ParameterResolutionException("Failed to create FileStore", e);
            }
        }, type);
    }

    @NotNull
    private static Path computePathForTest(ExtensionContext extensionContext) {
        try {
            final Class<?> clazz = extensionContext.getRequiredTestClass();
            final URI uri = clazz.getProtectionDomain().getCodeSource().getLocation().toURI();
            final Path pathToClassesFolder = Path.of(uri);
            return Stream.iterate(pathToClassesFolder, Path::getParent)
                    .limit(pathToClassesFolder.getNameCount())
                    .filter(p -> Objects.equals(p.getFileName().toString(), "target"))
                    .findFirst()
                    .map(p -> {
                        final String shortClassName = Stream.of(StringUtils.split(clazz.getPackageName(), '.'))
                                .map(name -> name.substring(0, 1))
                                .collect(Collectors.joining(".", "", "." + clazz.getSimpleName()));
                        final Path classDir = p.resolve("test-tmp").resolve(shortClassName);
                        Optional<Method> testMethod = extensionContext.getTestMethod();
                        if (testMethod.isPresent()) {
                            final Method method = testMethod.get();
                            String methodName = method.getName();
                            final Path methodDir = classDir.resolve(methodName);
                            final String params = testMethod
                                    .map(Method::getParameterTypes)
                                    .stream()
                                    .flatMap(Stream::of)
                                    .map(Class::getSimpleName)
                                    .collect(Collectors.joining(", ", "(", ")"));
                            final String methodWithParams = methodName + params;
                            if (Objects.equals(methodWithParams, extensionContext.getDisplayName())) {
                                return methodDir;
                            } else {
                                final String displayName = extensionContext.getDisplayName();
                                final String safeDirName = StringUtils.strip(displayName.replaceAll("\\W+", "-"), "-");
                                return methodDir.resolve(safeDirName);
                            }
                        } else {
                            ExtensionContext.Store store = extensionContext.getStore(NAMESPACE);
                            AtomicInteger counter = store.getOrComputeIfAbsent("static-counter", k -> new AtomicInteger(0), AtomicInteger.class);
                            return classDir.resolve("static-" + counter.getAndIncrement());
                        }
                    })
                    .orElseGet(() -> {
                        try {
                            return Files.createTempDirectory("test-tmp-fallback");
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        } catch (URISyntaxException e) {
            throw new ParameterResolutionException("Failed to compute temp dir path for test", e);
        }
    }

    // utility class to help with deleting temporary files
    private record CloseablePath(Path path) implements AutoCloseable {
        @Override
        public void close() throws Exception {
            PathUtils.delete(path);
        }
    }
}
