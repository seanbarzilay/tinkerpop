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

import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalExplanation;
import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;

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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.__;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests non-typed serialization/deserialization for GraphSON 2.0.
 */

public class GraphSONMapperV2d0Test {
    private final ObjectMapper mapper = GraphSONMapper.build()
            .version(GraphSONVersion.V2_0)
            .typeInfo(GraphSONMapper.TypeInfo.NO_TYPES)
            .create()
            .createMapper();

    @Test
    public void shouldHandleTraversalExplanation() throws Exception {
        final TraversalExplanation te = __().out().outV().outE().explain();
        final String json = mapper.writeValueAsString(te);
        assertEquals("{\"original\":[\"InjectStep([])\",\"VertexStep(OUT,vertex)\",\"EdgeVertexStep(OUT)\",\"VertexStep(OUT,edge)\"],\"intermediate\":[],\"final\":[\"InjectStep([])\",\"VertexStep(OUT,vertex)\",\"EdgeVertexStep(OUT)\",\"VertexStep(OUT,edge)\"]}", json);
    }

    @Test
    public void shouldHandleDuration() throws Exception {
        final Duration o = Duration.ZERO;
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleInstant() throws Exception {
        final Instant o = Instant.ofEpochMilli(System.currentTimeMillis());
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleLocalDate() throws Exception {
        final LocalDate o = LocalDate.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleLocalDateTime() throws Exception {
        final LocalDateTime o = LocalDateTime.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleLocalTime() throws Exception {
        final LocalTime o = LocalTime.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleMonthDay() throws Exception {
        final MonthDay o = MonthDay.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleOffsetDateTime() throws Exception {
        final OffsetDateTime o = OffsetDateTime.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleOffsetTime() throws Exception {
        final OffsetTime o = OffsetTime.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandlePeriod() throws Exception {
        final Period o = Period.ofDays(3);
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleYear() throws Exception {
        final Year o = Year.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleYearMonth() throws Exception {
        final YearMonth o = YearMonth.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleZonedDateTime() throws Exception {
        final ZonedDateTime o = ZonedDateTime.now();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldHandleZoneOffset() throws Exception {
        final ZoneOffset o = ZonedDateTime.now().getOffset();
        final String json = mapper.writeValueAsString(o);
        assertEquals("\"" + o.toString() + "\"", json);
    }

    @Test
    public void shouldSerializeDeserializeNestedCollectionsAndMapAndTypedCollectionsCorrectly() throws Exception {
        UUID uuid = UUID.randomUUID();
        List myList = new ArrayList<>();

        List myList2 = new ArrayList<>();
        myList2.add(UUID.randomUUID());
        myList2.add(33L);
        myList2.add(84);
        Map map2 = new HashMap<>();
        map2.put("eheh", UUID.randomUUID());
        map2.put("normal", "normal");
        myList2.add(map2);

        Map<String, Object> map1 = new HashMap<>();
        map1.put("hello", "world");
        map1.put("test", uuid);
        map1.put("hehe", myList2);
        myList.add(map1);

        myList.add("kjkj");
        myList.add(UUID.randomUUID());

        String json = mapper.writeValueAsString(myList);
        Object read = mapper.readValue(json, Object.class);

        // Not equals because of type loss
        assertNotEquals(myList, read);
    }

}
