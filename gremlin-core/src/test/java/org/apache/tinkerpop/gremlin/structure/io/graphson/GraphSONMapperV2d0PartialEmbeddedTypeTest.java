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

import org.apache.tinkerpop.shaded.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.time.*;
import java.util.*;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests automatic typed serialization/deserialization for GraphSON 2.0.
 */
public class GraphSONMapperV2d0PartialEmbeddedTypeTest {

    private final ObjectMapper mapper = GraphSONMapper.build()
            .version(GraphSONVersion.V2_0)
            .typeInfo(GraphSONMapper.TypeInfo.PARTIAL_TYPES)
            .create()
            .createMapper();

    @Test
    public void shouldHandleDuration()throws Exception  {
        final Duration o = Duration.ZERO;
        assertEquals(o, serializeDeserialize(o, Duration.class));
    }
    @Test
    public void shouldHandleInstant()throws Exception  {
        final Instant o = Instant.ofEpochMilli(System.currentTimeMillis());
        assertEquals(o, serializeDeserialize(o, Instant.class));
    }

    @Test
    public void shouldHandleLocalDate()throws Exception  {
        final LocalDate o = LocalDate.now();
        assertEquals(o, serializeDeserialize(o, LocalDate.class));
    }

    @Test
    public void shouldHandleLocalDateTime()throws Exception  {
        final LocalDateTime o = LocalDateTime.now();
        assertEquals(o, serializeDeserialize(o, LocalDateTime.class));
    }

    @Test
    public void shouldHandleLocalTime()throws Exception  {
        final LocalTime o = LocalTime.now();
        assertEquals(o, serializeDeserialize(o, LocalTime.class));
    }

    @Test
    public void shouldHandleMonthDay()throws Exception  {
        final MonthDay o = MonthDay.now();
        assertEquals(o, serializeDeserialize(o, MonthDay.class));
    }

    @Test
    public void shouldHandleOffsetDateTime()throws Exception  {
        final OffsetDateTime o = OffsetDateTime.now();
        assertEquals(o, serializeDeserialize(o, OffsetDateTime.class));
    }

    @Test
    public void shouldHandleOffsetTime()throws Exception  {
        final OffsetTime o = OffsetTime.now();
        assertEquals(o, serializeDeserialize(o, OffsetTime.class));
    }

    @Test
    public void shouldHandlePeriod()throws Exception  {
        final Period o = Period.ofDays(3);
        assertEquals(o, serializeDeserialize(o, Period.class));
    }

    @Test
    public void shouldHandleYear()throws Exception  {
        final Year o = Year.now();
        assertEquals(o, serializeDeserialize(o, Year.class));
    }

    @Test
    public void shouldHandleYearMonth()throws Exception  {
        final YearMonth o = YearMonth.now();
        assertEquals(o, serializeDeserialize(o, YearMonth.class));
    }

    @Test
    public void shouldHandleZonedDateTime()throws Exception  {
        final ZonedDateTime o = ZonedDateTime.now();
        assertEquals(o, serializeDeserialize(o, ZonedDateTime.class));
    }

    @Test
    public void shouldHandleZonedOffset()throws Exception  {
        final ZoneOffset o  = ZonedDateTime.now().getOffset();
        assertEquals(o, serializeDeserialize(o, ZoneOffset.class));
    }

    @Test
    public void shouldHandleDurationAuto() throws Exception {
        final Duration o = Duration.ZERO;
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleInstantAuto() throws Exception {
        final Instant o = Instant.ofEpochMilli(System.currentTimeMillis());
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleLocalDateAuto() throws Exception {
        final LocalDate o = LocalDate.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleLocalDateTimeAuto() throws Exception {
        final LocalDateTime o = LocalDateTime.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleLocalTimeAuto() throws Exception {
        final LocalTime o = LocalTime.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleMonthDayAuto() throws Exception {
        final MonthDay o = MonthDay.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleOffsetDateTimeAuto() throws Exception {
        final OffsetDateTime o = OffsetDateTime.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleOffsetTimeAuto() throws Exception {
        final OffsetTime o = OffsetTime.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandlePeriodAuto() throws Exception {
        final Period o = Period.ofDays(3);
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleYearAuto() throws Exception {
        final Year o = Year.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleYearMonthAuto() throws Exception {
        final YearMonth o = YearMonth.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleZonedDateTimeAuto() throws Exception {
        final ZonedDateTime o = ZonedDateTime.now();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldHandleZonedOffsetAuto() throws Exception {
        final ZoneOffset o = ZonedDateTime.now().getOffset();
        assertEquals(o, serializeDeserializeAuto(o));
    }

    @Test
    public void shouldSerializeDeserializeNestedCollectionsAndMapAndTypedValuesCorrectly() throws Exception {
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

        assertEquals(myList, serializeDeserializeAuto(myList));
    }

    @Test
    public void shouldFailIfTypeSpecifiedIsNotSameTypeInPayload() {
        final ZoneOffset o = ZonedDateTime.now().getOffset();
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        try {
            mapper.writeValue(stream, o);
            final InputStream inputStream = new ByteArrayInputStream(stream.toByteArray());
            // What has been serialized is a ZoneOffset with the type, but the user explicitly requires another type.
            mapper.readValue(inputStream, Instant.class);
            fail("Should have failed decoding the value");
        } catch (Exception e) {
            assertThat(e.getMessage(), containsString("Could not deserialize the JSON value as required. Cannot deserialize the value with the detected type contained in the JSON (\"ZoneOffset\") to the type specified in parameter to the object mapper (class java.time.Instant). Those types are incompatible."));
        }
    }

    @Test
    public void shouldHandleRawPOJOs() throws Exception {
        final FunObject funObject = new FunObject();
        funObject.setVal("test");
        assertEquals(funObject.toString(), serializeDeserialize(funObject, FunObject.class).toString());
        assertEquals(funObject.getClass(), serializeDeserialize(funObject, FunObject.class).getClass());
    }


    // Class needs to be defined as statics as it's a nested class.
    public static class FunObject {
        private String val;

        public FunObject() {
        }

        public String getVal() {
            return this.val;
        }

        public void setVal(String s) {
            this.val = s;
        }

        public String toString() {
            return this.val;
        }
    }

    public <T> T serializeDeserialize(final Object o, final Class<T> clazz) throws Exception {
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            mapper.writeValue(stream, o);

            try (final InputStream inputStream = new ByteArrayInputStream(stream.toByteArray())) {
                return mapper.readValue(inputStream, clazz);
            }
        }
    }

    public <T> T serializeDeserializeAuto(final Object o) throws Exception {
        try (final ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            mapper.writeValue(stream, o);

            try (final InputStream inputStream = new ByteArrayInputStream(stream.toByteArray())) {
                // Object.class is the wildcard that triggers the auto discovery.
                return (T)mapper.readValue(inputStream, Object.class);
            }
        }
    }

}
