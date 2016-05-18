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

import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.star.StarGraphGraphSONSerializer;
import org.apache.tinkerpop.shaded.jackson.databind.module.SimpleModule;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.Period;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Map;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Tree;

/**
 * The set of serializers that handle the core graph interfaces.  These serializers support normalization which
 * ensures that generated GraphSON will be compatible with line-based versioning tools. This setting comes with
 * some overhead, with respect to key sorting and other in-memory operations.
 * <p/>
 * This is a base class for grouping these core serializers.  Concrete extensions of this class represent a "version"
 * that should be registered with the {@link GraphSONVersion} enum.
 *
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
abstract class GraphSONModule extends SimpleModule {

    GraphSONModule(final String name) {
        super(name);
    }

    /**
     * Version 2.0 of GraphSON.
     */
    static final class GraphSONModuleV2d0 extends GraphSONModule {

        /**
         * Constructs a new object.
         */
        protected GraphSONModuleV2d0(final boolean normalize) {
            super("graphson-2.0");
            // graph
            addSerializer(Edge.class, new GraphSONSerializersV1d0.EdgeJacksonSerializer(normalize));
            addSerializer(Vertex.class, new GraphSONSerializersV1d0.VertexJacksonSerializer(normalize));
            addSerializer(VertexProperty.class, new GraphSONSerializersV1d0.VertexPropertyJacksonSerializer(normalize));
            addSerializer(Property.class, new GraphSONSerializersV1d0.PropertyJacksonSerializer());
            addSerializer(TraversalMetrics.class, new GraphSONSerializersV1d0.TraversalMetricsJacksonSerializer());
            addSerializer(TraversalExplanation.class, new GraphSONSerializersV1d0.TraversalExplanationJacksonSerializer());
            addSerializer(Path.class, new GraphSONSerializersV1d0.PathJacksonSerializer());
            addSerializer(StarGraphGraphSONSerializer.DirectionalStarGraph.class, new StarGraphGraphSONSerializer(normalize));
            addSerializer(Tree.class, new GraphSONSerializersV1d0.TreeJacksonSerializer());

            // java.util
            addSerializer(Map.Entry.class, new JavaUtilSerializersV1d0.MapEntryJacksonSerializer());

            // java.time
            addSerializer(Duration.class, new JavaTimeSerializersV1d0.DurationJacksonSerializer());
            addSerializer(Instant.class, new JavaTimeSerializersV1d0.InstantJacksonSerializer());
            addSerializer(LocalDate.class, new JavaTimeSerializersV1d0.LocalDateJacksonSerializer());
            addSerializer(LocalDateTime.class, new JavaTimeSerializersV1d0.LocalDateTimeJacksonSerializer());
            addSerializer(LocalTime.class, new JavaTimeSerializersV1d0.LocalTimeJacksonSerializer());
            addSerializer(MonthDay.class, new JavaTimeSerializersV1d0.MonthDayJacksonSerializer());
            addSerializer(OffsetDateTime.class, new JavaTimeSerializersV1d0.OffsetDateTimeJacksonSerializer());
            addSerializer(OffsetTime.class, new JavaTimeSerializersV1d0.OffsetTimeJacksonSerializer());
            addSerializer(Period.class, new JavaTimeSerializersV1d0.PeriodJacksonSerializer());
            addSerializer(Year.class, new JavaTimeSerializersV1d0.YearJacksonSerializer());
            addSerializer(YearMonth.class, new JavaTimeSerializersV1d0.YearMonthJacksonSerializer());
            addSerializer(ZonedDateTime.class, new JavaTimeSerializersV1d0.ZonedDateTimeJacksonSerializer());
            addSerializer(ZoneOffset.class, new JavaTimeSerializersV1d0.ZoneOffsetJacksonSerializer());

            addDeserializer(Duration.class, new JavaTimeSerializersV1d0.DurationJacksonDeserializer());
            addDeserializer(Instant.class, new JavaTimeSerializersV1d0.InstantJacksonDeserializer());
            addDeserializer(LocalDate.class, new JavaTimeSerializersV1d0.LocalDateJacksonDeserializer());
            addDeserializer(LocalDateTime.class, new JavaTimeSerializersV1d0.LocalDateTimeJacksonDeserializer());
            addDeserializer(LocalTime.class, new JavaTimeSerializersV1d0.LocalTimeJacksonDeserializer());
            addDeserializer(MonthDay.class, new JavaTimeSerializersV1d0.MonthDayJacksonDeserializer());
            addDeserializer(OffsetDateTime.class, new JavaTimeSerializersV1d0.OffsetDateTimeJacksonDeserializer());
            addDeserializer(OffsetTime.class, new JavaTimeSerializersV1d0.OffsetTimeJacksonDeserializer());
            addDeserializer(Period.class, new JavaTimeSerializersV1d0.PeriodJacksonDeserializer());
            addDeserializer(Year.class, new JavaTimeSerializersV1d0.YearJacksonDeserializer());
            addDeserializer(YearMonth.class, new JavaTimeSerializersV1d0.YearMonthJacksonDeserializer());
            addDeserializer(ZonedDateTime.class, new JavaTimeSerializersV1d0.ZonedDateTimeJacksonDeserializer());
            addDeserializer(ZoneOffset.class, new JavaTimeSerializersV1d0.ZoneOffsetJacksonDeserializer());
        }

        public static Builder build() {
            return new Builder();
        }

        static final class Builder implements GraphSONModuleBuilder {

            private Builder() {}

            @Override
            public GraphSONModule create(final boolean normalize) {
                return new GraphSONModuleV2d0(normalize);
            }
        }
    }

    /**
     * Version 1.0 of GraphSON.
     */
    static final class GraphSONModuleV1d0 extends GraphSONModule {

        /**
         * Constructs a new object.
         */
        protected GraphSONModuleV1d0(final boolean normalize) {
            super("graphson-1.0");
            // graph
            addSerializer(Edge.class, new GraphSONSerializersV1d0.EdgeJacksonSerializer(normalize));
            addSerializer(Vertex.class, new GraphSONSerializersV1d0.VertexJacksonSerializer(normalize));
            addSerializer(VertexProperty.class, new GraphSONSerializersV1d0.VertexPropertyJacksonSerializer(normalize));
            addSerializer(Property.class, new GraphSONSerializersV1d0.PropertyJacksonSerializer());
            addSerializer(TraversalMetrics.class, new GraphSONSerializersV1d0.TraversalMetricsJacksonSerializer());
            addSerializer(TraversalExplanation.class, new GraphSONSerializersV1d0.TraversalExplanationJacksonSerializer());
            addSerializer(Path.class, new GraphSONSerializersV1d0.PathJacksonSerializer());
            addSerializer(StarGraphGraphSONSerializer.DirectionalStarGraph.class, new StarGraphGraphSONSerializer(normalize));
            addSerializer(Tree.class, new GraphSONSerializersV1d0.TreeJacksonSerializer());
           
            // java.util
            addSerializer(Map.Entry.class, new JavaUtilSerializersV1d0.MapEntryJacksonSerializer());

            // java.time
            addSerializer(Duration.class, new JavaTimeSerializersV1d0.DurationJacksonSerializer());
            addSerializer(Instant.class, new JavaTimeSerializersV1d0.InstantJacksonSerializer());
            addSerializer(LocalDate.class, new JavaTimeSerializersV1d0.LocalDateJacksonSerializer());
            addSerializer(LocalDateTime.class, new JavaTimeSerializersV1d0.LocalDateTimeJacksonSerializer());
            addSerializer(LocalTime.class, new JavaTimeSerializersV1d0.LocalTimeJacksonSerializer());
            addSerializer(MonthDay.class, new JavaTimeSerializersV1d0.MonthDayJacksonSerializer());
            addSerializer(OffsetDateTime.class, new JavaTimeSerializersV1d0.OffsetDateTimeJacksonSerializer());
            addSerializer(OffsetTime.class, new JavaTimeSerializersV1d0.OffsetTimeJacksonSerializer());
            addSerializer(Period.class, new JavaTimeSerializersV1d0.PeriodJacksonSerializer());
            addSerializer(Year.class, new JavaTimeSerializersV1d0.YearJacksonSerializer());
            addSerializer(YearMonth.class, new JavaTimeSerializersV1d0.YearMonthJacksonSerializer());
            addSerializer(ZonedDateTime.class, new JavaTimeSerializersV1d0.ZonedDateTimeJacksonSerializer());
            addSerializer(ZoneOffset.class, new JavaTimeSerializersV1d0.ZoneOffsetJacksonSerializer());

            addDeserializer(Duration.class, new JavaTimeSerializersV1d0.DurationJacksonDeserializer());
            addDeserializer(Instant.class, new JavaTimeSerializersV1d0.InstantJacksonDeserializer());
            addDeserializer(LocalDate.class, new JavaTimeSerializersV1d0.LocalDateJacksonDeserializer());
            addDeserializer(LocalDateTime.class, new JavaTimeSerializersV1d0.LocalDateTimeJacksonDeserializer());
            addDeserializer(LocalTime.class, new JavaTimeSerializersV1d0.LocalTimeJacksonDeserializer());
            addDeserializer(MonthDay.class, new JavaTimeSerializersV1d0.MonthDayJacksonDeserializer());
            addDeserializer(OffsetDateTime.class, new JavaTimeSerializersV1d0.OffsetDateTimeJacksonDeserializer());
            addDeserializer(OffsetTime.class, new JavaTimeSerializersV1d0.OffsetTimeJacksonDeserializer());
            addDeserializer(Period.class, new JavaTimeSerializersV1d0.PeriodJacksonDeserializer());
            addDeserializer(Year.class, new JavaTimeSerializersV1d0.YearJacksonDeserializer());
            addDeserializer(YearMonth.class, new JavaTimeSerializersV1d0.YearMonthJacksonDeserializer());
            addDeserializer(ZonedDateTime.class, new JavaTimeSerializersV1d0.ZonedDateTimeJacksonDeserializer());
            addDeserializer(ZoneOffset.class, new JavaTimeSerializersV1d0.ZoneOffsetJacksonDeserializer());
        }

        public static Builder build() {
            return new Builder();
        }

        static final class Builder implements GraphSONModuleBuilder {

            private Builder() {}

            @Override
            public GraphSONModule create(final boolean normalize) {
                return new GraphSONModuleV1d0(normalize);
            }
        }
    }

    /**
     * A "builder" used to create {@link GraphSONModule} instances.  Each "version" should have an associated
     * {@code GraphSONModuleBuilder} so that it can be registered with the {@link GraphSONVersion} enum.
     */
    static interface GraphSONModuleBuilder {

        /**
         * Creates a new {@link GraphSONModule} object.
         *
         * @param normalize when set to true, keys and objects are ordered to ensure that they are the occur in
         *                  the same order.
         */
        GraphSONModule create(final boolean normalize);
    }
}
