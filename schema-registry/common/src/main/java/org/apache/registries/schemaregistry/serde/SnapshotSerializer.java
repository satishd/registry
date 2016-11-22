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
package org.apache.registries.schemaregistry.serde;

import org.apache.registries.schemaregistry.Resourceable;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Serializer interface for serializing input {@code I} into output {@code O} according to the Schema {@code S}.
 *
 * @param <I> Input type of the payload
 * @param <O> serialized output type. For ex: byte[], String etc.
 * @param <S> schema related information, which can be used for Input to be serialized as Output
 */
public interface SnapshotSerializer<I, O, S> extends Resourceable {

    /**
     * Returns an instance of O which is serialized input according to the given schema
     *
     * @param input
     * @param schema
     * @return
     */
    O serialize(I input, S schema) throws SerDesException;

    /**
     * Serializes the given {@code input} and writes to the given {@code output} according to the given {@code schema}.
     *
     * @param input
     * @param output
     * @param schema
     * @throws SerDesException when any error occurs while serializing the given input.
     */
    void serialize(InputStream input, OutputStream output, S schema) throws SerDesException;

}
