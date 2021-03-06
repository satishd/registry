/**
 * Copyright 2016 Hortonworks.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package com.hortonworks.registries.schemaregistry.serde;

import com.hortonworks.registries.schemaregistry.errors.SchemaNotFoundException;
import com.hortonworks.registries.schemaregistry.SchemaIdVersion;
import com.hortonworks.registries.schemaregistry.errors.IncompatibleSchemaException;
import com.hortonworks.registries.schemaregistry.errors.InvalidSchemaException;
import com.hortonworks.registries.schemaregistry.SchemaMetadata;
import com.hortonworks.registries.schemaregistry.SchemaVersion;
import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;

import java.util.Map;

/**
 *
 */
public abstract class AbstractSnapshotSerializer<I, O> implements SnapshotSerializer<I, O, SchemaMetadata> {
    protected SchemaRegistryClient schemaRegistryClient;

    public AbstractSnapshotSerializer() {
    }

    @Override
    public void init(Map<String, ?> config) {
        schemaRegistryClient = new SchemaRegistryClient(config);
    }

    @Override
    public final O serialize(I input, SchemaMetadata schemaMetadata) throws SerDesException {

        // compute schema based on input object
        String schema = getSchemaText(input);

        // register that schema and get the version
        try {
            SchemaIdVersion schemaIdVersion = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema, "Schema registered by serializer:" + this.getClass()));
            // write the version and given object to the output
            return doSerialize(input, schemaIdVersion);
        } catch (InvalidSchemaException | IncompatibleSchemaException | SchemaNotFoundException e) {
            throw new SerDesException(e);
        }
    }

    protected abstract String getSchemaText(I input);

    protected abstract O doSerialize(I input, SchemaIdVersion schemaIdVersion) throws SerDesException;

    @Override
    public void close() throws Exception {
        schemaRegistryClient.close();
    }
}
