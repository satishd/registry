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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.schemaregistry.samples.serdes;

import org.apache.registries.schemaregistry.SchemaMetadata;
import org.apache.registries.schemaregistry.serde.SerDesException;
import org.apache.registries.schemaregistry.serde.SnapshotDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.OutputStream;
import java.util.Map;

/**
 * This is a very simple deserializer, it does not really connect to schema-registry and register schemas. This
 * is used to demonstrate how this serializer can be uploaded/retrieved from schema registry.
 * <p>
 * You can look at {@link org.apache.registries.schemaregistry.avro.AvroSnapshotSerializer} implementation to look
 * at how schema registry client can be used to register writer schemas which are used while deserializing messages from
 * the respective {@link org.apache.registries.schemaregistry.avro.AvroSnapshotDeserializer}.
 */
public class SimpleDeserializer implements SnapshotDeserializer<byte[], Object, SchemaMetadata, Integer> {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleDeserializer.class);

    @Override
    public void init(Map<String, ?> config) {
        LOG.info("This is initialized with config: [{}]", config);
    }

    @Override
    public Object deserialize(byte[] input, SchemaMetadata writerSchemaMetadata, Integer readerSchemaVersion) throws SerDesException {
        LOG.info("Received payload [{}] to be deserialized with writer schema [{}] and reader schema: [{}]", input, writerSchemaMetadata, readerSchemaVersion);

        try {
            ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(input));
            return ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deserialize(InputStream input, OutputStream output, SchemaMetadata writerSchemaInfo, Integer readerSchemaInfo) throws SerDesException {
        throw new UnsupportedOperationException("This API is not supported.");
    }

    @Override
    public void close() throws Exception {
        LOG.info("Closing deserializer to release any resources.");

    }
}
