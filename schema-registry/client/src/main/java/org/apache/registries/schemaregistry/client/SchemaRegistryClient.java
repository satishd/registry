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
package org.apache.registries.schemaregistry.client;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import org.apache.registries.common.catalog.CatalogResponse;
import org.apache.registries.common.util.ClassLoaderAwareInvocationHandler;
import org.apache.registries.schemaregistry.ConfigEntry;
import org.apache.registries.schemaregistry.SchemaFieldQuery;
import org.apache.registries.schemaregistry.SchemaIdVersion;
import org.apache.registries.schemaregistry.SchemaMetadata;
import org.apache.registries.schemaregistry.SchemaMetadataInfo;
import org.apache.registries.schemaregistry.SchemaProviderInfo;
import org.apache.registries.schemaregistry.SchemaVersion;
import org.apache.registries.schemaregistry.SchemaVersionInfo;
import org.apache.registries.schemaregistry.SchemaVersionInfoCache;
import org.apache.registries.schemaregistry.SchemaVersionKey;
import org.apache.registries.schemaregistry.SerDesInfo;
import org.apache.registries.schemaregistry.errors.IncompatibleSchemaException;
import org.apache.registries.schemaregistry.errors.InvalidSchemaException;
import org.apache.registries.schemaregistry.errors.SchemaNotFoundException;
import org.apache.registries.schemaregistry.serde.SerDesException;
import org.apache.registries.schemaregistry.serde.SnapshotDeserializer;
import org.apache.registries.schemaregistry.serde.SnapshotSerializer;
import org.apache.registries.schemaregistry.serde.pull.PullDeserializer;
import org.apache.registries.schemaregistry.serde.pull.PullSerializer;
import org.apache.registries.schemaregistry.serde.push.PushDeserializer;
import org.glassfish.jersey.client.ClientConfig;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.media.multipart.BodyPart;
import org.glassfish.jersey.media.multipart.MultiPart;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import org.glassfish.jersey.media.multipart.file.StreamDataBodyPart;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.registries.schemaregistry.client.SchemaRegistryClient.Configuration.DEFAULT_CONNECTION_TIMEOUT;
import static org.apache.registries.schemaregistry.client.SchemaRegistryClient.Configuration.DEFAULT_READ_TIMEOUT;
import static org.apache.registries.schemaregistry.client.SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL;

/**
 * This is the default implementation of {@link ISchemaRegistryClient} which connects to the given {@code rootCatalogURL}.
 * <p>
 * An instance of SchemaRegistryClient can be instantiated
 * <ul>
 *     <li> By passing configuration properties map like below. <br/>
 *         <pre>{@code Map<String, ?> config = ...
 *SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(config);}</pre>
 *      <li> By giving configuration file containing properties as mentioned in {@link Configuration}
 *         <pre>{@code File configFile = ...
 *SchemaRegistryClient schemaRegistryClient = SchemaRegistryClient.of(configFile);}</pre>
 *       <li> By giving inputstream with properties as mentioned in {@link Configuration}
 *         <pre>{@code InputStream configInputStream = ...
 *SchemaRegistryClient schemaRegistryClient = SchemaRegistryClient.of(configInputStream);}</pre>
 * </ul>
 * <p>
 * There are different configuration properties available as mentioned in {@link Configuration} like
 * <pre>
 * - {@link Configuration#SCHEMA_REGISTRY_URL}.
 * - {@link Configuration#SCHEMA_METADATA_CACHE_SIZE}.
 * - {@link Configuration#SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS}.
 * - {@link Configuration#SCHEMA_VERSION_CACHE_SIZE}.
 * - {@link Configuration#SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS}.
 * - {@link Configuration#SCHEMA_TEXT_CACHE_SIZE}.
 * - {@link Configuration#SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS}.
 *
 * and many other properties like {@link ClientProperties}
 * </pre>
 *
 * Below code describes how to register new schemas, add new version of a schema and fetch different versions of a schema.
 * <pre>
 * {@code
 * SchemaMetadata  schemaMetadata =  new SchemaMetadata.Builder(name)
 * .type(AvroSchemaProvider.TYPE)
 * .schemaGroup("sample-group")
 * .description("Sample schema")
 * .compatibility(SchemaProvider.Compatibility.BACKWARD)
 * .build();
 *
 * // registering a new schema
 * Integer v1 = schemaRegistryClient.addSchemaVersion(schemaMetadata, new SchemaVersion(schema1, "Initial version of the schema"));
 * LOG.info("Registered schema [{}] and returned version [{}]", schema1, v1);
 *
 * // adding a new version of the schema
 * String schema2 = getSchema("/device-next.avsc");
 * SchemaVersion schemaInfo2 = new SchemaVersion(schema2, "second version");
 * Integer v2 = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2);
 * LOG.info("Registered schema [{}] and returned version [{}]", schema2, v2);
 *
 * //adding same schema returns the earlier registered version
 * Integer version = schemaRegistryClient.addSchemaVersion(schemaMetadata, schemaInfo2);
 * LOG.info("");
 *
 * // get a specific version of the schema
 * String schemaName = schemaMetadata.getName();
 * SchemaVersionInfo schemaVersionInfo = schemaRegistryClient.getSchemaVersionInfo(new SchemaVersionKey(schemaName, v2));
 *
 * // get latest version of the schema
 * SchemaVersionInfo latest = schemaRegistryClient.getLatestSchemaVersionInfo(schemaName);
 * LOG.info("Latest schema with schema key [{}] is : [{}]", schemaMetadata, latest);
 *
 * // get all versions of the schema
 * Collection<SchemaVersionInfo> allVersions = schemaRegistryClient.getAllVersions(schemaName);
 * LOG.info("All versions of schema key [{}] is : [{}]", schemaMetadata, allVersions);
 *
 * // finding schemas containing a specific field
 * SchemaFieldQuery md5FieldQuery = new SchemaFieldQuery.Builder().name("md5").build();
 * Collection<SchemaVersionKey> md5SchemaVersionKeys = schemaRegistryClient.findSchemasByFields(md5FieldQuery);
 * LOG.info("Schemas containing field query [{}] : [{}]", md5FieldQuery, md5SchemaVersionKeys);
 *
 * SchemaFieldQuery txidFieldQuery = new SchemaFieldQuery.Builder().name("txid").build();
 * Collection<SchemaVersionKey> txidSchemaVersionKeys = schemaRegistryClient.findSchemasByFields(txidFieldQuery);
 * LOG.info("Schemas containing field query [{}] : [{}]", txidFieldQuery, txidSchemaVersionKeys);
 *
 * // Default serializer and deserializer for a given schema provider can be retrieved with the below APIs.
 * // for avro,
 * AvroSnapshotSerializer serializer = schemaRegistryClient.getDefaultSerializer(AvroSchemaProvider.TYPE);
 * AvroSnapshotDeserializer deserializer = schemaRegistryClient.getDefaultDeserializer(AvroSchemaProvider.TYPE);
 * }</pre>
 *
 *
 * Below code describes how to register serializer and deserializers, map them with a schema etc.
 * <pre> {@code
 * // upload a jar containing serializer and deserializer classes.
 * InputStream inputStream = new FileInputStream("/schema-custom-ser-des.jar");
 * String fileId = schemaRegistryClient.uploadFile(inputStream);
 *
 * // add serializer with the respective uploaded jar file id.
 * SerDesInfo serializerInfo = createSerDesInfo(fileId);
 * Long serializerId = schemaRegistryClient.addSerializer(serializerInfo);
 *
 * // map this serializer with a registered schema
 * schemaRegistryClient.mapSchemaWithSerDes(schemaName, serializerId);
 *
 * // get registered serializers
 * Collection<SerDesInfo> serializers = schemaRegistryClient.getSerializers(schemaName);
 * SerDesInfo registeredSerializerInfo = serializers.iterator().next();
 *
 * //get serializer and serialize the given payload
 * try(AvroSnapshotSerializer snapshotSerializer = schemaRegistryClient.createInstance(registeredSerializerInfo);) {
 *   Map<String, Object> config = Collections.emptyMap();
 *   snapshotSerializer.init(config);
 *
 *   byte[] serializedData = snapshotSerializer.serialize(input, schemaInfo);
 * }
 * }</pre>
 *
 */
public class SchemaRegistryClient implements ISchemaRegistryClient {
    private static final Logger LOG = LoggerFactory.getLogger(SchemaRegistryClient.class);

    private static final String SCHEMA_REGISTRY_PATH = "/schemaregistry";
    private static final String SCHEMAS_PATH = SCHEMA_REGISTRY_PATH + "/schemas/";
    private static final String SCHEMA_PROVIDERS_PATH = SCHEMA_REGISTRY_PATH + "/schemaproviders/";
    private static final String SCHEMAS_BY_ID_PATH = SCHEMA_REGISTRY_PATH + "/schemasById/";
    private static final String FILES_PATH = SCHEMA_REGISTRY_PATH + "/files/";
    private static final String SERIALIZERS_PATH = SCHEMA_REGISTRY_PATH + "/serializers/";
    private static final String DESERIALIZERS_PATH = SCHEMA_REGISTRY_PATH + "/deserializers/";
    private static final Set<Class<?>> DESERIALIZER_INTERFACE_CLASSES = Sets.<Class<?>>newHashSet(SnapshotDeserializer.class, PullDeserializer.class, PushDeserializer.class);
    private static final Set<Class<?>> SERIALIZER_INTERFACE_CLASSES = Sets.<Class<?>>newHashSet(SnapshotSerializer.class, PullSerializer.class);

    private final Client client;
    private final WebTarget rootTarget;
    private final WebTarget schemasTarget;
    private final WebTarget schemasByIdTarget;
    private final WebTarget searchFieldsTarget;
    private final WebTarget schemaProvidersTarget;

    private final Configuration configuration;
    private final ClassLoaderCache classLoaderCache;
    private final SchemaVersionInfoCache schemaVersionInfoCache;
    private final SchemaMetadataCache schemaMetadataCache;
    private final Cache<SchemaDigestEntry, SchemaIdVersion> schemaTextCache;

    public SchemaRegistryClient(Map<String, ?> conf) {
        configuration = new Configuration(conf);

        ClientConfig config = createClientConfig(conf);
        client = ClientBuilder.newBuilder().withConfig(config).build();
        client.register(MultiPartFeature.class);

        String rootCatalogURL = configuration.getValue(SCHEMA_REGISTRY_URL.name());
        rootTarget = client.target(rootCatalogURL);
        schemaProvidersTarget = rootTarget.path(SCHEMA_PROVIDERS_PATH);
        schemasTarget = rootTarget.path(SCHEMAS_PATH);
        schemasByIdTarget = rootTarget.path(SCHEMAS_BY_ID_PATH);
        searchFieldsTarget = schemasTarget.path("search/fields");

        classLoaderCache = new ClassLoaderCache(this);

        schemaVersionInfoCache = new SchemaVersionInfoCache(key -> doGetSchemaVersionInfo(key),
                                                            configuration.getValue(Configuration.SCHEMA_VERSION_CACHE_SIZE.name()),
                                                            configuration.getValue(Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name()));

        SchemaMetadataCache.SchemaMetadataFetcher schemaMetadataFetcher = createSchemaMetadataFetcher();
        schemaMetadataCache = new SchemaMetadataCache(configuration.getValue(Configuration.SCHEMA_METADATA_CACHE_SIZE.name()),
                                                      configuration.getValue(Configuration.SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS.name()),
                                                      schemaMetadataFetcher);

        schemaTextCache = CacheBuilder.newBuilder()
                .maximumSize(configuration.getValue(Configuration.SCHEMA_TEXT_CACHE_SIZE.name()))
                .expireAfterAccess(configuration.getValue(Configuration.SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS.name()), TimeUnit.MILLISECONDS)
                .build();
    }

    private SchemaMetadataCache.SchemaMetadataFetcher createSchemaMetadataFetcher() {
        return new SchemaMetadataCache.SchemaMetadataFetcher() {
            @Override
            public SchemaMetadataInfo fetch(String name) throws SchemaNotFoundException {
                try {
                    return getEntity(schemasTarget.path(name), SchemaMetadataInfo.class);
                } catch (NotFoundException e) {
                    throw new SchemaNotFoundException(e);
                }
            }

            @Override
            public SchemaMetadataInfo fetch(Long id) throws SchemaNotFoundException {
                try {
                    return getEntity(schemasByIdTarget.path(id.toString()), SchemaMetadataInfo.class);
                } catch (NotFoundException e) {
                    throw new SchemaNotFoundException(e);
                }
            }
        };
    }

    protected ClientConfig createClientConfig(Map<String, ?> conf) {
        ClientConfig config = new ClientConfig();
        config.property(ClientProperties.CONNECT_TIMEOUT, DEFAULT_CONNECTION_TIMEOUT);
        config.property(ClientProperties.READ_TIMEOUT, DEFAULT_READ_TIMEOUT);
        config.property(ClientProperties.FOLLOW_REDIRECTS, true);
        for (Map.Entry<String, ?> entry : conf.entrySet()) {
            config.property(entry.getKey(), entry.getValue());
        }
        return config;
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public Collection<SchemaProviderInfo> getSupportedSchemaProviders() {
        return getEntities(schemaProvidersTarget, SchemaProviderInfo.class);
    }

    @Override
    public Long registerSchemaMetadata(SchemaMetadata schemaMetadata) {
        SchemaMetadataInfo schemaMetadataInfo = schemaMetadataCache.getIfPresent(SchemaMetadataCache.Key.of(schemaMetadata.getName()));
        if (schemaMetadataInfo == null) {
            return doRegisterSchemaMetadata(schemaMetadata, schemasTarget);
        }

        return schemaMetadataInfo.getId();
    }

    private Long doRegisterSchemaMetadata(SchemaMetadata schemaMetadata, WebTarget schemasTarget) {
        return postEntity(schemasTarget, schemaMetadata, Long.class);
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(String schemaName) {
        return schemaMetadataCache.get(SchemaMetadataCache.Key.of(schemaName));
    }

    @Override
    public SchemaMetadataInfo getSchemaMetadataInfo(Long schemaMetadataId) {
        return schemaMetadataCache.get(SchemaMetadataCache.Key.of(schemaMetadataId));
    }

    @Override
    public SchemaIdVersion addSchemaVersion(SchemaMetadata schemaMetadata, SchemaVersion schemaVersion) throws
            InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {
        // get it, if it exists in cache
        SchemaDigestEntry schemaDigestEntry = buildSchemaTextEntry(schemaVersion, schemaMetadata.getName());
        SchemaIdVersion schemaIdVersion = schemaTextCache.getIfPresent(schemaDigestEntry);

        if (schemaIdVersion == null) {
            //register schema metadata if it does not exist
            Long metadataId = registerSchemaMetadata(schemaMetadata);
            if (metadataId == null) {
                LOG.error("Schema Metadata [{}] is not registered successfully", schemaMetadata);
                throw new RuntimeException("Given SchemaMetadata could not be registered: " + schemaMetadata);
            }

            // add schemaIdVersion
            schemaIdVersion = addSchemaVersion(schemaMetadata.getName(), schemaVersion);
        }

        return schemaIdVersion;
    }

    private SchemaDigestEntry buildSchemaTextEntry(SchemaVersion schemaVersion, String name) {
        byte[] digest;
        try {
            digest = MessageDigest.getInstance("MD5").digest(schemaVersion.getSchemaText().getBytes("UTF-8"));
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
            throw new RuntimeException(e.getMessage(), e);
        }

        // storing schema text string is expensive, so storing digest in cache's key.
        return new SchemaDigestEntry(name, digest);
    }

    @Override
    public SchemaIdVersion addSchemaVersion(final String schemaName, final SchemaVersion schemaVersion)
            throws InvalidSchemaException, IncompatibleSchemaException, SchemaNotFoundException {

        try {
            return schemaTextCache.get(buildSchemaTextEntry(schemaVersion, schemaName), new Callable<SchemaIdVersion>() {
                @Override
                public SchemaIdVersion call() throws Exception {
                    return doAddSchemaVersion(schemaName, schemaVersion);
                }
            });
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            LOG.error("Encountered error while adding new version [{}] of schema [{}] and error [{}]", schemaVersion, schemaName, e);
            if (cause != null) {
                if (cause instanceof InvalidSchemaException)
                    throw (InvalidSchemaException) cause;
                else if (cause instanceof IncompatibleSchemaException) {
                    throw (IncompatibleSchemaException) cause;
                } else if (cause instanceof SchemaNotFoundException) {
                    throw (SchemaNotFoundException) cause;
                } else {
                    throw new RuntimeException(cause.getMessage(), cause);
                }
            } else {
                throw new RuntimeException(e.getMessage(), e);
            }
        }
    }

    private SchemaIdVersion doAddSchemaVersion(String schemaName, SchemaVersion schemaVersion) throws IncompatibleSchemaException, InvalidSchemaException, SchemaNotFoundException {
        SchemaMetadataInfo schemaMetadataInfo = getSchemaMetadataInfo(schemaName);
        if (schemaMetadataInfo == null) {
            throw new SchemaNotFoundException("Schema with name " + schemaName + " not found");
        }

        WebTarget target = schemasTarget.path(schemaName).path("/versions");
        Response response = target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(schemaVersion), Response.class);
        int status = response.getStatus();
        String msg = response.readEntity(String.class);
        if (status == Response.Status.BAD_REQUEST.getStatusCode() || status == Response.Status.INTERNAL_SERVER_ERROR.getStatusCode()) {
            CatalogResponse catalogResponse = readCatalogResponse(msg);
            if (CatalogResponse.ResponseMessage.INCOMPATIBLE_SCHEMA.getCode() == catalogResponse.getResponseCode()) {
                throw new IncompatibleSchemaException(catalogResponse.getResponseMessage());
            } else if (CatalogResponse.ResponseMessage.INVALID_SCHEMA.getCode() == catalogResponse.getResponseCode()) {
                throw new InvalidSchemaException(catalogResponse.getResponseMessage());
            } else {
                throw new RuntimeException(catalogResponse.getResponseMessage());
            }

        }

        return new SchemaIdVersion(schemaMetadataInfo.getId(), readEntity(msg, Integer.class));
    }

    private CatalogResponse readCatalogResponse(String msg) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            JsonNode node = objectMapper.readTree(msg);

            return objectMapper.treeToValue(node, CatalogResponse.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public SchemaVersionInfo getSchemaVersionInfo(SchemaVersionKey schemaVersionKey) throws SchemaNotFoundException {
        try {
            return schemaVersionInfoCache.getSchema(schemaVersionKey);
        } catch (SchemaNotFoundException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private SchemaVersionInfo doGetSchemaVersionInfo(SchemaVersionKey schemaVersionKey) {
        String schemaName = schemaVersionKey.getSchemaName();
        WebTarget webTarget = schemasTarget.path(String.format("%s/versions/%d", schemaName, schemaVersionKey.getVersion()));

        return getEntity(webTarget, SchemaVersionInfo.class);
    }

    @Override
    public SchemaVersionInfo getLatestSchemaVersionInfo(String schemaName) throws SchemaNotFoundException {
        WebTarget webTarget = schemasTarget.path(schemaName + "/versions/latest");
        return getEntity(webTarget, SchemaVersionInfo.class);
    }

    @Override
    public Collection<SchemaVersionInfo> getAllVersions(String schemaName) throws SchemaNotFoundException {
        WebTarget webTarget = schemasTarget.path(schemaName + "/versions");
        return getEntities(webTarget, SchemaVersionInfo.class);
    }

    @Override
    public boolean isCompatibleWithAllVersions(String schemaName, String toSchemaText) throws SchemaNotFoundException {
        WebTarget webTarget = schemasTarget.path(schemaName + "/compatibility");
        String response = webTarget.request().post(Entity.text(toSchemaText), String.class);
        return readEntity(response, Boolean.class);
    }

    @Override
    public Collection<SchemaVersionKey> findSchemasByFields(SchemaFieldQuery schemaFieldQuery) {
        WebTarget target = searchFieldsTarget;
        for (Map.Entry<String, String> entry : schemaFieldQuery.toQueryMap().entrySet()) {
            target = target.queryParam(entry.getKey(), entry.getValue());
        }

        return getEntities(target, SchemaVersionKey.class);
    }

    @Override
    public String uploadFile(InputStream inputStream) {
        MultiPart multiPart = new MultiPart();
        BodyPart filePart = new StreamDataBodyPart("file", inputStream, "file");
        multiPart.bodyPart(filePart);

        String response = rootTarget.path(FILES_PATH).request().post(Entity.entity(multiPart, MediaType.MULTIPART_FORM_DATA), String.class);
        return readEntity(response, String.class);
    }

    @Override
    public InputStream downloadFile(String fileId) {
        return rootTarget.path(FILES_PATH).path("download/" + fileId).request().get(InputStream.class);
    }

    @Override
    public Long addSerializer(SerDesInfo serializerInfo) {
        return postEntity(rootTarget.path(SERIALIZERS_PATH), serializerInfo, Long.class);
    }

    @Override
    public Long addDeserializer(SerDesInfo deserializerInfo) {
        return postEntity(rootTarget.path(DESERIALIZERS_PATH), deserializerInfo, Long.class);
    }

    @Override
    public void mapSchemaWithSerDes(String schemaName, Long serDesId) {
        String path = String.format("%s/mapping/%s", schemaName, serDesId.toString());

        Boolean success = postEntity(schemasTarget.path(path), null, Boolean.class);
        LOG.info("Received response while mapping schema [{}] with serialzer/deserializer [{}] : [{}]", schemaName, serDesId, success);
    }

    @Override
    public <T> T getDefaultSerializer(String type) throws SerDesException {
        Collection<SchemaProviderInfo> supportedSchemaProviders = getSupportedSchemaProviders();
        for (SchemaProviderInfo schemaProvider : supportedSchemaProviders) {
            if (schemaProvider.getType().equals(type)) {
                try {
                    return (T) Class.forName(schemaProvider.getDefaultSerializerClassName()).newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new SerDesException(e);
                }
            }
        }

        throw new IllegalArgumentException("No schema provider registered for the given type " + type);
    }

    @Override
    public <T> T getDefaultDeserializer(String type) throws SerDesException {
        Collection<SchemaProviderInfo> supportedSchemaProviders = getSupportedSchemaProviders();
        for (SchemaProviderInfo schemaProvider : supportedSchemaProviders) {
            if (schemaProvider.getType().equals(type)) {
                try {
                    return (T) Class.forName(schemaProvider.getDefaultDeserializerClassName()).newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new SerDesException(e);
                }
            }
        }

        throw new IllegalArgumentException("No schema provider registered for the given type " + type);
    }

    @Override
    public Collection<SerDesInfo> getSerializers(String schemaName) {
        String path = schemaName + "/serializers/";
        return getEntities(schemasTarget.path(path), SerDesInfo.class);
    }

    @Override
    public Collection<SerDesInfo> getDeserializers(String schemaName) {
        String path = schemaName + "/deserializers/";
        return getEntities(schemasTarget.path(path), SerDesInfo.class);
    }

    public <T> T createSerializerInstance(SerDesInfo serializerInfo) {
        return createInstance(serializerInfo, SERIALIZER_INTERFACE_CLASSES);
    }

    @Override
    public <T> T createDeserializerInstance(SerDesInfo deserializerInfo) {
        return createInstance(deserializerInfo, DESERIALIZER_INTERFACE_CLASSES);
    }

    @Override
    public void close() {
        client.close();
    }

    private <T> T createInstance(SerDesInfo serDesInfo, Set<Class<?>> interfaceClasses) {
        if (interfaceClasses == null || interfaceClasses.isEmpty()) {
            throw new IllegalArgumentException("interfaceClasses array must be neither null nor empty.");
        }

        // loading serializer, create a class loader and and keep them in cache.
        String fileId = serDesInfo.getFileId();
        // get class loader for this file ID
        ClassLoader classLoader = classLoaderCache.getClassLoader(fileId);

        T t;
        try {
            String className = serDesInfo.getClassName();
            Class<T> clazz = (Class<T>) Class.forName(className, true, classLoader);
            t = clazz.newInstance();
            List<Class<?>> classes = new ArrayList<>();
            for (Class<?> interfaceClass : interfaceClasses) {
                if (interfaceClass.isAssignableFrom(clazz)) {
                    classes.add(interfaceClass);
                }
            }

            if (classes.isEmpty()) {
                throw new RuntimeException("Given Serialize/Deserializer " + className + " class does not implement any " +
                                                   "one of the registered interfaces: " + interfaceClasses);
            }

            Proxy.newProxyInstance(Thread.currentThread().getContextClassLoader(),
                                   classes.toArray(new Class[classes.size()]),
                                   new ClassLoaderAwareInvocationHandler(classLoader, t));
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new SerDesException(e);
        }

        return t;
    }

    private <T> List<T> getEntities(WebTarget target, Class<T> clazz) {
        List<T> entities = new ArrayList<>();
        String response = target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response);
            Iterator<JsonNode> it = node.get("entities").elements();
            while (it.hasNext()) {
                entities.add(mapper.treeToValue(it.next(), clazz));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return entities;
    }

    private <T> T postEntity(WebTarget target, Object json, Class<T> responseType) {
        String response = target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(json), String.class);

        return readEntity(response, responseType);
    }

    private <T> T readEntity(String response, Class<T> clazz) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response);
            return mapper.treeToValue(node.get("entity"), clazz);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private <T> T getEntity(WebTarget target, Class<T> clazz) {
        String response = target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);

        return readEntity(response, clazz);
    }

    /**
     * Returns {@link SchemaRegistryClient} with the given configuration file.
     *
     * @param configFile configuration file containing properties as mentioned in {@link Configuration}
     * @throws IOException when any IO error(including file not found) occurs while reading from the given {@code inputStream}
     */
    public static SchemaRegistryClient of(File configFile) throws IOException {
        Preconditions.checkNotNull("Given configFile must not be null");

        try (FileInputStream fis = new FileInputStream(configFile)) {
            return of(fis);
        }
    }

    /**
     * Returns {@link SchemaRegistryClient} with the configuration passed with {@code inputStream}.
     *
     * @param inputStream InputStream containing properties as mentioned in {@link Configuration}
     * @throws IOException when any IO error occurs while reading from the given {@code inputStream}
     */
    public static SchemaRegistryClient of(InputStream inputStream) throws IOException {
        Preconditions.checkNotNull("Given inputStream must not be null");

        ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());
        Map<String, ?> props = objectMapper.readValue(inputStream, new TypeReference<Map<String, ?>>() {});
        return new SchemaRegistryClient(props);
    }

    public static final class Configuration {
        // we may want to remove schema.registry prefix from configuration properties as these are all properties
        // given by client.
        /**
         * URL of schema registry to which this client connects to. For ex: http://localhost:9090/api/v1
         */
        public static final ConfigEntry<String> SCHEMA_REGISTRY_URL =
                ConfigEntry.mandatory("schema.registry.url",
                                      String.class,
                                      "URL of schema registry to which this client connects to. For ex: http://localhost:9090/api/v1",
                                      "http://localhost:9090/api/v1",
                                      ConfigEntry.NonEmptyStringValidator.get());

        /**
         * Default path for downloaded jars to be stored.
         */
        public static final String DEFAULT_LOCAL_JARS_PATH = "/tmp/schema-registry/local-jars";

        /**
         * Local directory path to which downloaded jars should be copied to. For ex: /tmp/schema-registry/local-jars
         */
        public static final ConfigEntry<String> LOCAL_JAR_PATH =
                ConfigEntry.optional("schema.registry.client.local.jars.path",
                                     String.class,
                                     "URL of schema registry to which this client connects to. For ex: http://localhost:9090/api/v1",
                                     DEFAULT_LOCAL_JARS_PATH,
                                     ConfigEntry.NonEmptyStringValidator.get());

        /**
         * Default value for classloader cache size.
         */
        public static final long DEFAULT_CLASSLOADER_CACHE_SIZE = 1024L;

        /**
         * Default value for cache expiry interval in seconds.
         */
        public static final long DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS = 60 * 60L;

        /**
         * Maximum size of classloader cache. Default value is {@link #DEFAULT_CLASSLOADER_CACHE_SIZE}
         * Classloaders are created for serializer/deserializer jars downloaded from schema registry and they will be locally cached.
         */
        public static final ConfigEntry<Number> CLASSLOADER_CACHE_SIZE =
                ConfigEntry.optional("schema.registry.client.class.loader.cache.size",
                                     Long.class,
                                     "Maximum size of classloader cache",
                                     DEFAULT_CLASSLOADER_CACHE_SIZE,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         * Expiry interval(in seconds) of an entry in classloader cache. Default value is {@link #DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS}
         * Classloaders are created for serializer/deserializer jars downloaded from schema registry and they will be locally cached.
         */
        public static final ConfigEntry<Number> CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS =
                ConfigEntry.optional("schema.registry.client.class.loader.cache.expiry.interval.secs",
                                     Long.class,
                                     "Expiry interval(in seconds) of an entry in classloader cache",
                                     DEFAULT_CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS,
                                     ConfigEntry.PositiveNumberValidator.get());

        public static final long DEFAULT_SCHEMA_CACHE_SIZE = 1024L;
        public static final long DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS = 5 * 60L;

        /**
         * Maximum size of schema version cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_SIZE}
         */
        public static final ConfigEntry<Number> SCHEMA_VERSION_CACHE_SIZE =
                ConfigEntry.optional("schema.registry.client.schema.version.cache.size",
                                     Long.class,
                                     "Maximum size of schema version cache",
                                     DEFAULT_SCHEMA_CACHE_SIZE,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         * Expiry interval(in seconds) of an entry in schema version cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS}
         */
        public static final ConfigEntry<Number> SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS =
                ConfigEntry.optional("schema.registry.client.schema.version.cache.expiry.interval.secs",
                                     Long.class,
                                     "Expiry interval(in seconds) of an entry in schema version cache",
                                     DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         * Maximum size of schema metadata cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_SIZE}
         */
        public static final ConfigEntry<Number> SCHEMA_METADATA_CACHE_SIZE =
                ConfigEntry.optional("schema.registry.client.schema.metadata.cache.size",
                                     Long.class,
                                     "Maximum size of schema metadata cache",
                                     DEFAULT_SCHEMA_CACHE_SIZE,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         * Expiry interval(in seconds) of an entry in schema metadata cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS}
         */
        public static final ConfigEntry<Number> SCHEMA_METADATA_CACHE_EXPIRY_INTERVAL_SECS =
                ConfigEntry.optional("schema.registry.client.schema.metadata.cache.expiry.interval.secs",
                                     Long.class,
                                     "Expiry interval(in seconds) of an entry in schema metadata cache",
                                     DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         * Maximum size of schema text cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_SIZE}.
         * This cache has ability to store/get entries with same schema name and schema text.
         */
        public static final ConfigEntry<Number> SCHEMA_TEXT_CACHE_SIZE =
                ConfigEntry.optional("schema.registry.client.schema.text.cache.size",
                                     Long.class,
                                     "Maximum size of schema text cache",
                                     DEFAULT_SCHEMA_CACHE_SIZE,
                                     ConfigEntry.PositiveNumberValidator.get());

        /**
         * Expiry interval(in seconds) of an entry in schema text cache. Default value is {@link #DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS}
         */
        public static final ConfigEntry<Number> SCHEMA_TEXT_CACHE_EXPIRY_INTERVAL_SECS =
                ConfigEntry.optional("schema.registry.client.schema.text.cache.expiry.interval.secs",
                                     Long.class,
                                     "Expiry interval(in seconds) of an entry in schema text cache.",
                                     DEFAULT_SCHEMA_CACHE_EXPIRY_INTERVAL_SECS,
                                     ConfigEntry.PositiveNumberValidator.get());

        // connection properties
        /**
         * Default connection timeout on connections created while connecting to schema registry.
         */
        public static final int DEFAULT_CONNECTION_TIMEOUT = 30 * 1000;

        /**
         * Default read timeout on connections created while connecting to schema registry.
         */
        public static final int DEFAULT_READ_TIMEOUT = 30 * 1000;

        private final Map<String, ?> config;

        private static final Map<String, ConfigEntry<?>> options =
                Collections.unmodifiableMap(buildOptions(Configuration.class.getDeclaredFields()));

        public Configuration(Map<String, ?> config) {
            this.config = buildConfig(config);
        }

        private Map<String, ?> buildConfig(Map<String, ?> config) {
            Map<String, Object> result = new HashMap<>();
            for (Map.Entry<String, ?> entry : config.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();

                ConfigEntry configEntry = options.get(key);
                if (configEntry != null) {
                    if (value != null) {
                        configEntry.validator().validate((value));
                    } else {
                        value = configEntry.defaultValue();
                    }
                }
                result.put(key, value);
            }

            return result;
        }

        private static Map<String, ConfigEntry<?>> buildOptions(Field[] fields) {
            Map<String, ConfigEntry<?>> options = new HashMap<>();
            Configuration configInstance = new Configuration(Collections.emptyMap());
            for (Field field : fields) {
                Class<?> type = field.getType();

                if (type.isAssignableFrom(ConfigEntry.class)) {
                    field.setAccessible(true);
                    try {
                        ConfigEntry configEntry = (ConfigEntry) field.get(configInstance);
                        options.put(configEntry.name(), configEntry);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return options;
        }

        public <T> T getValue(String propertyKey) {
            return (T) (config.containsKey(propertyKey) ? config.get(propertyKey) : options.get(propertyKey).defaultValue());
        }

        /**
         * @return an unmodifiable {@link Map} of effective properties for this instance.
         */
        public Map<String, ?> getProperties() {
            return Collections.unmodifiableMap(config);
        }

        /**
         * @return Collection of {@link ConfigEntry} which are defined by schema registry client. This does not include
         * properties like jersey client configuration.
         */
        public static Collection<ConfigEntry<?>> getAvailableConfigEntries() {
            return options.values();
        }

    }

    private static class SchemaDigestEntry {
        private final String name;
        private final byte[] schemaDigest;

        SchemaDigestEntry(String name, byte[] schemaDigest) {
            Preconditions.checkNotNull(name, "name can not be null");
            Preconditions.checkNotNull(schemaDigest, "schema digest can not be null");

            this.name = name;
            this.schemaDigest = schemaDigest;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            SchemaDigestEntry that = (SchemaDigestEntry) o;

            if (name != null ? !name.equals(that.name) : that.name != null) return false;
            return Arrays.equals(schemaDigest, that.schemaDigest);

        }

        @Override
        public int hashCode() {
            int result = name != null ? name.hashCode() : 0;
            result = 31 * result + Arrays.hashCode(schemaDigest);
            return result;
        }
    }
}
