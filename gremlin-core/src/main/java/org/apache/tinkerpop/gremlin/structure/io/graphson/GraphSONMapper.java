/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.tinkerpop.gremlin.structure.io.graphson;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoRegistry;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonTypeInfo;
import org.apache.tinkerpop.shaded.jackson.core.JsonGenerator;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.apache.tinkerpop.shaded.jackson.databind.SerializationFeature;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeResolverBuilder;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.impl.StdTypeResolverBuilder;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;
import org.apache.tinkerpop.shaded.jackson.databind.ser.DefaultSerializerProvider;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An extension to the standard Jackson {@code ObjectMapper} which automatically registers the standard
 * {@link GraphSONModule} for serializing {@link Graph} elements.  This class
 * can be used for generalized JSON serialization tasks that require meeting GraphSON standards.
 * <p/>
 * {@link Graph} implementations providing an {@link IoRegistry} should register their {@code SimpleModule}
 * implementations to it as follows:
 * <pre>
 * {@code
 * public class MyGraphIoRegistry extends AbstractIoRegistry {
 *   public MyGraphIoRegistry() {
 *     register(GraphSONIo.class, null, new MyGraphSimpleModule());
 *   }
 * }
 * }
 * </pre>
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONMapper implements Mapper<ObjectMapper> {

    private final List<SimpleModule> customModules;
    private final boolean loadCustomSerializers;
    private final boolean normalize;
    private final boolean embedTypes;
    private final GraphSONVersion version;
    private final TypeInfo typeInfo;

    private GraphSONMapper(final Builder builder) {
        this.customModules = builder.customModules;
        this.loadCustomSerializers = builder.loadCustomModules;
        this.normalize = builder.normalize;
        this.embedTypes = builder.embedTypes;
        this.version = builder.version;
        this.typeInfo = builder.typeInfo;
    }

    @Override
    public ObjectMapper createMapper() {
        final ObjectMapper om = new ObjectMapper();
        om.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        GraphSONModule graphSONModule = version.getBuilder().create(normalize);
        om.registerModule(graphSONModule);
        customModules.forEach(om::registerModule);


        // plugin external serialization modules
        if (loadCustomSerializers)
            om.findAndRegisterModules();


        if (version == GraphSONVersion.V1_0) {
            if (embedTypes) {
                final TypeResolverBuilder<?> typer = new StdTypeResolverBuilder()
                        .init(JsonTypeInfo.Id.CLASS, null)
                        .inclusion(JsonTypeInfo.As.PROPERTY)
                        .typeProperty(GraphSONTokens.CLASS);
                om.setDefaultTyping(typer);
            }
        } else if (version == GraphSONVersion.V2_0) {
            if (typeInfo != TypeInfo.NO_TYPES) {
                GraphSONTypeIdResolver graphSONTypeIdResolver = new GraphSONTypeIdResolver();
                final TypeResolverBuilder typer = new GraphSONTypeResolverBuilder()
                        .typesEmbedding(getTypeInfo())
                        .init(JsonTypeInfo.Id.CUSTOM, graphSONTypeIdResolver)
                        .typeProperty(GraphSONTokens.CLASS);

                // Register types to typeResolver for the GraphSON module
                for (Map.Entry<String, Class> typeDeser : graphSONModule.getAddedDeserializers().entrySet()) {
                    graphSONTypeIdResolver.addCustomType(typeDeser.getKey(), typeDeser.getValue());
                }

                // Register types to typeResolver for the Custom modules
                customModules.forEach(e -> {
                    if (e instanceof TinkerPopJacksonModule) {
                        for (Map.Entry<String, Class> typeDeser : ((TinkerPopJacksonModule) e).getAddedDeserializers().entrySet()) {
                            graphSONTypeIdResolver.addCustomType(typeDeser.getKey(), typeDeser.getValue());
                        }
                    }
                });
                om.setDefaultTyping(typer);
            }
        } else {
            throw new IllegalStateException("Unknown GraphSONVersion : " + version);
        }

        // this provider toStrings all unknown classes and converts keys in Map objects that are Object to String.
        final DefaultSerializerProvider provider = new GraphSONSerializerProvider(version);
        om.setSerializerProvider(provider);

        if (normalize)
            om.enable(SerializationFeature.ORDER_MAP_ENTRIES_BY_KEYS);

        // keep streams open to accept multiple values (e.g. multiple vertices)
        om.getFactory().disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        return om;
    }

    public GraphSONVersion getVersion() {
        return this.version;
    }

    public static Builder build() {
        return new Builder();
    }

    public TypeInfo getTypeInfo() {
        return this.typeInfo;
    }

    public enum TypeInfo {
        NO_TYPES,
        PARTIAL_TYPES
    }

    public static class Builder implements Mapper.Builder<Builder> {
        private List<SimpleModule> customModules = new ArrayList<>();
        private boolean loadCustomModules = false;
        private boolean normalize = false;
        private boolean embedTypes = false;
        private List<IoRegistry> registries = new ArrayList<>();
        private GraphSONVersion version = GraphSONVersion.V1_0;
        // GraphSON 2.0 should have types activated by default, otherwise use there's no point in using it instead of 1.0.
        private TypeInfo typeInfo = TypeInfo.PARTIAL_TYPES;

        private Builder() {
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Builder addRegistry(final IoRegistry registry) {
            registries.add(registry);
            return this;
        }

        /**
         * Set the version of GraphSON to use.
         */
        public Builder version(final GraphSONVersion version) {
            this.version = version;
            return this;
        }

        /**
         * Set the version of GraphSON to use.
         */
        public Builder version(final String version) {
            this.version = GraphSONVersion.valueOf(version);
            return this;
        }

        /**
         * Supply a mapper module for serialization/deserialization.
         */
        public Builder addCustomModule(final SimpleModule custom) {
            this.customModules.add(custom);
            return this;
        }

        /**
         * Try to load {@code SimpleModule} instances from the current classpath.  These are loaded in addition to
         * the one supplied to the {@link #addCustomModule(SimpleModule)};
         */
        public Builder loadCustomModules(final boolean loadCustomModules) {
            this.loadCustomModules = loadCustomModules;
            return this;
        }

        /**
         * Forces keys to be sorted.
         */
        public Builder normalize(final boolean normalize) {
            this.normalize = normalize;
            return this;
        }

        /**
         * Embeds Java types into generated JSON to clarify their origins.
         */
        public Builder embedTypes(final boolean embedTypes) {
            this.embedTypes = embedTypes;
            return this;
        }

        /**
         * Specify if the values are going to be typed or not, and at which level.
         *
         * The level can be NO_TYPES or PARTIAL_TYPES, and could be extended in the future.
         */
        public Builder typeInfo(final TypeInfo typeInfo) {
            this.typeInfo = typeInfo;
            return this;
        }

        public GraphSONMapper create() {
            registries.forEach(registry -> {
                final List<Pair<Class, SimpleModule>> simpleModules = registry.find(GraphSONIo.class, SimpleModule.class);
                simpleModules.stream().map(Pair::getValue1).forEach(this.customModules::add);
            });

            return new GraphSONMapper(this);
        }

    }
}
