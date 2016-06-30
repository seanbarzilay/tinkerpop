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
package org.apache.tinkerpop.gremlin.tinkergraph.structure;

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;
import org.apache.tinkerpop.gremlin.structure.io.IoTest;
import org.apache.tinkerpop.gremlin.structure.io.Mapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONReader;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONVersion;
import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONWriter;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;

import static org.junit.Assert.assertEquals;


public class TinkerGraphGraphSONSerializerV2d0Test {

    TinkerGraph baseModern = TinkerFactory.createModern();

    // As of TinkerPop 3.2.1 default for GraphSON 2.0 means types enabled.
    Mapper defaultMapperV2d0 = GraphSONMapper.build()
            .version(GraphSONVersion.V2_0)
            .addRegistry(TinkerIoRegistryV2d0.getInstance())
            .create();

    Mapper noTypesMapperV2d0 = GraphSONMapper.build()
            .version(GraphSONVersion.V2_0)
            .typeInfo(GraphSONMapper.TypeInfo.NO_TYPES)
            .addRegistry(TinkerIoRegistryV2d0.getInstance())
            .create();

    @Test
    public void shouldDeserializeGraphSONIntoTinkerGraphWithPartialTypes() throws IOException {
        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, baseModern);
            String json = out.toString();
            TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            IoTest.assertModernGraph(read, true, false);
        }
    }

    @Test
    public void shouldDeserializeGraphSONIntoTinkerGraphWithoutTypes() throws IOException {
        GraphWriter writer = getWriter(noTypesMapperV2d0);
        GraphReader reader = getReader(noTypesMapperV2d0);
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, baseModern);
            String json = out.toString();
            TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            IoTest.assertModernGraph(read, true, false);
        }
    }

    @Test
    public void shouldKeepTypesWhenDeserializingSerializedTinkerGraph() throws IOException {
        TinkerGraph tg = TinkerGraph.open();

        Vertex v = tg.addVertex("vertexTest");
        UUID uuidProp = UUID.randomUUID();
        Duration durationProp = Duration.ofHours(3);
        Long longProp = 2L;
        ByteBuffer byteBufferProp = ByteBuffer.wrap("testbb".getBytes());

        // One Java util type natively supported by Jackson
        v.property("uuid", uuidProp);
        // One custom time type added by the GraphSON module
        v.property("duration", durationProp);
        // One Java native type not handled by JSON natively
        v.property("long", longProp);
        // One Java util type added by GraphSON
        v.property("bytebuffer", byteBufferProp);

        GraphWriter writer = getWriter(defaultMapperV2d0);
        GraphReader reader = getReader(defaultMapperV2d0);
        try (final ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            writer.writeGraph(out, tg);
            String json = out.toString();
            TinkerGraph read = TinkerGraph.open();
            reader.readGraph(new ByteArrayInputStream(json.getBytes()), read);
            Vertex vRead = read.traversal().V().hasLabel("vertexTest").next();
            assertEquals(vRead.property("uuid").value(), uuidProp);
            assertEquals(vRead.property("duration").value(), durationProp);
            assertEquals(vRead.property("long").value(), longProp);
            assertEquals(vRead.property("bytebuffer").value(), byteBufferProp);
        }
    }

    private GraphWriter getWriter(Mapper paramMapper) {
        return GraphSONWriter.build().mapper(paramMapper).create();
    }

    private GraphReader getReader(Mapper paramMapper) {
        return GraphSONReader.build().mapper(paramMapper).create();
    }
}
