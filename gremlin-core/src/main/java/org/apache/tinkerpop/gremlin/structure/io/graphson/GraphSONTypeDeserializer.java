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

import org.apache.tinkerpop.gremlin.structure.io.graphson.GraphSONMapper.TypeInfo;
import org.apache.tinkerpop.shaded.jackson.annotation.JsonTypeInfo;
import org.apache.tinkerpop.shaded.jackson.core.JsonParser;
import org.apache.tinkerpop.shaded.jackson.core.JsonToken;
import org.apache.tinkerpop.shaded.jackson.databind.BeanProperty;
import org.apache.tinkerpop.shaded.jackson.databind.DeserializationContext;
import org.apache.tinkerpop.shaded.jackson.databind.JavaType;
import org.apache.tinkerpop.shaded.jackson.databind.JsonDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeDeserializer;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.TypeIdResolver;
import org.apache.tinkerpop.shaded.jackson.databind.jsontype.impl.TypeDeserializerBase;
import org.apache.tinkerpop.shaded.jackson.databind.type.TypeFactory;
import org.apache.tinkerpop.shaded.jackson.databind.util.TokenBuffer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Contains main logic for the whole JSON to Java deserialization. Handles types embedded with the version 2.0 of GraphSON.
 *
 * @author Kevin Gallardo (https://kgdo.me)
 */
public class GraphSONTypeDeserializer extends TypeDeserializerBase {
    private final TypeIdResolver idRes;
    private final String propertyName;
    private final JavaType baseType;
    private final TypeInfo typeInfo;

    private static final JavaType mapJavaType = TypeFactory.defaultInstance().constructType(Map.class);
    private static final JavaType arrayJavaType = TypeFactory.defaultInstance().constructType(List.class);


    GraphSONTypeDeserializer(JavaType baseType, TypeIdResolver idRes, String typePropertyName,
                             TypeInfo typeInfo){
        super(baseType, idRes, typePropertyName, false, null);
        this.baseType = baseType;
        this.idRes = idRes;
        this.propertyName = typePropertyName;
        this.typeInfo = typeInfo;
    }

    @Override
    public TypeDeserializer forProperty(BeanProperty beanProperty) {
        return this;
    }

    @Override
    public JsonTypeInfo.As getTypeInclusion() {
        return JsonTypeInfo.As.WRAPPER_ARRAY;
    }


    @Override
    public TypeIdResolver getTypeIdResolver() {
        return idRes;
    }

    @Override
    public Class<?> getDefaultImpl() {
        return null;
    }

    @Override
    public Object deserializeTypedFromObject(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        return deserialize(jsonParser, deserializationContext);
    }

    @Override
    public Object deserializeTypedFromArray(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        return deserialize(jsonParser, deserializationContext);
    }

    @Override
    public Object deserializeTypedFromScalar(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        return deserialize(jsonParser, deserializationContext);
    }

    @Override
    public Object deserializeTypedFromAny(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        return deserialize(jsonParser, deserializationContext);
    }

    /**
     * Main logic for the deserialization.
     */
    private Object deserialize(JsonParser jsonParser, DeserializationContext deserializationContext) throws IOException {
        TokenBuffer buf = new TokenBuffer(jsonParser.getCodec(), false);

        // Detect type
        try {
            // The Type pattern is START_ARRAY -> START_OBJECT -> TEXT_FIELD(propertyName).
            if (jsonParser.getCurrentToken() == JsonToken.START_ARRAY) {
                buf.writeStartArray();
                JsonToken t1 = jsonParser.nextToken();

                if (t1 == JsonToken.START_OBJECT) {
                    buf.writeStartObject();
                    String nextFieldName = jsonParser.nextFieldName();

                    if (nextFieldName != null) {
                        if (nextFieldName.equals(propertyName)) {
                            // Detected type pattern
                            // Find deserializer
                            // Deserialize with deserializer
                            String typeName = jsonParser.nextTextValue();
                            JavaType typeFromId = idRes.typeFromId(typeName);

                            // Detected a type and extracted the value, but still want to make sure that the extracted type
                            // corresponds to what was given in parameter, if a type has been explicitly specified in param.
                            if (!baseType.isJavaLangObject() && baseType != typeFromId) {
                                throw new InstantiationException(
                                        String.format("Cannot deserialize the value with the detected type contained in the JSON (\"%s\") " +
                                                "to the type specified in parameter to the object mapper (%s). " +
                                                "Those types are incompatible.", typeName, baseType.getRawClass().toString())
                                );
                            }

                            JsonDeserializer jsonDeserializer = deserializationContext.findContextualValueDeserializer(typeFromId, null);

                            // Position the next token right on the value
                            if (jsonParser.nextToken() == JsonToken.END_OBJECT) {
                                // Locate the cursor on the value to deser
                                jsonParser.nextValue();
                                Object value = jsonDeserializer.deserialize(jsonParser, deserializationContext);
                                // IMPORTANT - Close the JSON ARRAY
                                jsonParser.nextToken();
                                return value;
                            }
                        }
                    }

                }
            }
        } catch (Exception e) {
            throw deserializationContext.mappingException("Could not deserialize the JSON value as required. " + e.getMessage());
        }

        JsonParser toUseParser;
        JsonParser bufferParser = buf.asParser();
        JsonToken t = bufferParser.nextToken();

        // While searching for the type pattern, we may have moved the cursor of the original JsonParser in param.
        // To compensate, we have filled consistently a TokenBuffer that should contain the equivalent of
        // what we skipped while searching for the pattern.
        // This has an huge positive impact on performances, since JsonParser does not have a 'rewind()',
        // the only other solution would have been to copy the whole original JsonParser. Which we avoid here and use
        // an efficient structure made of TokenBuffer + JsonParserSequence/Concat.
        if (t != null)
            toUseParser = JsonParserConcat.createFlattened(bufferParser, jsonParser);

        // If the cursor hasn't been moved, no need to concatenate the original JsonParser with the TokenBuffer's one.
        else
            toUseParser = jsonParser;

        // If a type has been specified in parameter :
        if (!baseType.isJavaLangObject()) {
            JsonDeserializer jsonDeserializer = deserializationContext.findContextualValueDeserializer(baseType, null);
            return jsonDeserializer.deserialize(toUseParser, deserializationContext);
        }
        // Otherwise, detect the current structure :
        else {
            if (toUseParser.isExpectedStartArrayToken()) {
                return deserializationContext.findContextualValueDeserializer(arrayJavaType, null).deserialize(toUseParser, deserializationContext);
            } else if (toUseParser.isExpectedStartObjectToken()) {
                return deserializationContext.findContextualValueDeserializer(mapJavaType, null).deserialize(toUseParser, deserializationContext);
            } else {
                // There's JavaLangObject in param, there's no type detected in the payload, the payload isn't a JSON Map or JSON List
                // then consider it a simple type, even though we shouldn't be here if it was a simple type.
                // TODO : maybe throw an error instead?
                // throw deserializationContext.mappingException("Roger, we have a problem deserializing");
                JsonDeserializer jsonDeserializer = deserializationContext.findContextualValueDeserializer(baseType, null);
                return jsonDeserializer.deserialize(toUseParser, deserializationContext);
            }
        }
    }

    private boolean canReadTypeId() {
        return this.typeInfo == TypeInfo.PARTIAL_TYPES;
    }
}
