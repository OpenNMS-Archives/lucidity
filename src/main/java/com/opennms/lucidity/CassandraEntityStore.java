/*
 * Copyright 2013, The OpenNMS Group
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *     
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.opennms.lucidity;

import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static com.opennms.lucidity.Schema.ENTITY;
import static com.opennms.lucidity.Schema.ID;
import static com.opennms.lucidity.Schema.INDEX;
import static com.opennms.lucidity.Schema.indexTableName;
import static com.opennms.lucidity.Schema.joinColumnName;
import static com.opennms.lucidity.Schema.joinTableName;
import static java.lang.String.format;

import java.io.IOException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;


// FIXME: Cache schemas
// FIXME: Support collection types.

/**
 * Apache Cassandra implementation of {@link EntityStore}.
 * 
 * @author eevans
 */
public class CassandraEntityStore implements EntityStore {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraEntityStore.class);

    private final Session m_session;
    private final ConsistencyLevel m_consistency;
    private final ConcurrentMap<Integer, Record> m_instanceCache = Maps.newConcurrentMap();

    private boolean m_isOpen;

    public CassandraEntityStore(Session session, ConsistencyLevel consistency) {
        m_session = session;
        m_consistency = consistency;
        m_isOpen = true;
    }

    private Schema getSchema(Object object) {
        return Schema.fromClass(object.getClass());
    }

    private com.datastax.driver.core.ConsistencyLevel getDriverConsistencyLevel(ConsistencyLevel cl) {
        return com.datastax.driver.core.ConsistencyLevel.fromCode(cl.getDriverCode());
    }

    private Integer getInstanceID(Object o) {
        return System.identityHashCode(o);
    }

    @Override
    public <T> T create(T object) {
        return create(object, m_consistency);
    }

    @Override
    public <T> T create(T object, ConsistencyLevel consistency) {

        checkNotNull(object, "object argument");
        checkNotNull(consistency, "consistency argument");
        checkState(m_isOpen, format("%s is closed", getClass().getSimpleName()));
        checkArgument(
                object.getClass().isAnnotationPresent(ENTITY),
                format("%s not annotated with @%s", getClass().getSimpleName(), ENTITY.getCanonicalName()));

        Schema schema = getSchema(object);

        checkArgument(
                schema.getIDValue(object) == null,
                format("property annotated with @%s must be null", ID.getCanonicalName()));

        // Object persistence (incl. indices)
        UUID id = UUID.randomUUID();
        Batch batch = batch();
        Insert insertStatement = insertInto(schema.getTableName()).value(schema.getIDName(), id);

        for (Entry<String, Field> entry : schema.getColumns().entrySet()) {
            String columnName = entry.getKey();
            Field f = entry.getValue();
            
            insertStatement.value(columnName, schema.getColumnValue(columnName, object));
            
            if (entry.getValue().isAnnotationPresent(Schema.INDEX)) {
                String tableName = indexTableName(schema.getTableName(), columnName);
                batch.add(
                        insertInto(tableName)
                            .value(columnName, Util.getFieldValue(f, object))
                            .value(joinColumnName(schema.getTableName()), id)
                );
            }
        }

        batch.add(insertStatement);

        // One-to-Many relationship persistence
        for (Map.Entry<Field, Schema> entry : schema.getOneToManys().entrySet()) {
            Schema s = entry.getValue();

            Object relations = Util.getFieldValue(entry.getKey(), object);

            if (relations == null) {
                continue;
            }

            for (Object item : (Collection<?>) relations) {
                UUID relationID = (UUID) s.getIDValue(item);

                if (relationID == null) {
                    throw new IllegalStateException(
                            "encountered relation with null ID property (entity not persisted?)");
                }

                String joinTable = joinTableName(schema.getTableName(), s.getTableName());

                batch.add(
                        insertInto(joinTable)
                            .value(joinColumnName(schema.getTableName()), id)
                            .value(joinColumnName(s.getTableName()), relationID)
                );
            }

        }

        executeStatement(batch, consistency);

        Util.setFieldValue(schema.getIDField(), object, id);
        cacheInstance(object);
        
        return object;
    }

    @Override
    public <T> void update(T object) {
        update(object, m_consistency);
    }

    @Override
    public <T> void update(T object, ConsistencyLevel consistency) {

        checkNotNull(object, "object argument");
        checkNotNull(consistency, "consistency argument");
        checkState(m_isOpen, format("%s is closed", getClass().getSimpleName()));

        Record record = m_instanceCache.get(getInstanceID(object));

        if (record == null) {
            throw new IllegalStateException("untracked object");
        }

        Schema schema = getSchema(object);
        boolean needsUpdate = false;

        Update updateStatement = QueryBuilder.update(schema.getTableName());
        Batch batchStatement = batch();

        for (String columnName : schema.getColumns().keySet()) {

            Object past, current;
            current = schema.getColumnValue(columnName, object);
            past = record.getColumns().get(columnName);

            if (current != null && !current.equals(past)) {
                needsUpdate = true;
                updateStatement.with(set(columnName, current));

                // Update index, if applicable
                if (schema.getColumns().get(columnName).isAnnotationPresent(INDEX)) {
                    batchStatement.add(
                            QueryBuilder.update(indexTableName(schema.getTableName(), columnName))
                                    .with(set(joinColumnName(schema.getTableName()), schema.getIDValue(object)))
                                    .where(eq(columnName, schema.getColumnValue(columnName, object)))
                    );
                }
            }
        }

        updateStatement.where(eq(schema.getIDName(), schema.getIDValue(object)));

        if (needsUpdate) {
            batchStatement.add(updateStatement);
        }

        for (Map.Entry<Field, Schema> entry : schema.getOneToManys().entrySet()) {
            Field f = entry.getKey();
            Schema s = entry.getValue();

            Collection<?> past, current;
            current = (Collection<?>)Util.getFieldValue(f, object);
            past = (record != null) ? record.getOneToManys().get(f) : null;

            if (current == null) {
                current = Collections.emptySet();
            }
            
            if (past == null) {
                past = Collections.emptySet();
            }

            SetView<?> toInsert = Sets.difference(Sets.newHashSet(current), Sets.newHashSet(past));
            SetView<?> toRemove = Sets.difference(Sets.newHashSet(past), Sets.newHashSet(current));

            String joinTable = joinTableName(schema.getTableName(), s.getTableName());

            for (Object o : toInsert) {
                if (s.getIDValue(o) == null) {
                    throw new IllegalStateException(
                            "encountered relation with null ID property (entity not persisted?)");
                }
                batchStatement.add(insertInto(joinTable)
                        .value(joinColumnName(schema.getTableName()), schema.getIDValue(object))
                        .value(joinColumnName(s.getTableName()), s.getIDValue(o)));
            }

            for (Object o : toRemove) {
                if (s.getIDValue(o) == null) {
                    throw new IllegalStateException(
                            "encountered relation with null ID property (entity not persisted?)");
                }
                batchStatement.add(
                        QueryBuilder.delete().from(joinTable)
                            .where(eq(joinColumnName(schema.getTableName()), schema.getIDValue(object)))
                                .and(eq(joinColumnName(s.getTableName()), s.getIDValue(o)))
                );
            }
        }

        executeStatement(batchStatement, consistency);
        cacheInstance(object);

    }

    @Override
    public <T> Optional<T> read(Class<T> cls, UUID id) {
        return read(cls, id, m_consistency);
    }

    @Override
    public <T> Optional<T> read(Class<T> cls, UUID id, ConsistencyLevel consistency) {

        checkNotNull(cls, "class argument");
        checkNotNull(id, "id argument");
        checkNotNull(consistency, "consistency argument");
        checkState(m_isOpen, format("%s is closed", getClass().getSimpleName()));

        T instance = Util.newInstance(cls);

        Schema schema = getSchema(instance);
        Statement selectStatement = select().from(schema.getTableName()).where(eq(schema.getIDName(), id));
        selectStatement.setConsistencyLevel(getDriverConsistencyLevel(consistency));
        ResultSet results = executeStatement(selectStatement, consistency);
        Row row = results.one();

        checkState(results.isExhausted(), "query returned more than one row");
        if (row == null) return Optional.absent();

        Util.setFieldValue(schema.getIDField(), instance, row.getUUID(schema.getIDName()));

        for (String columnName : schema.getColumns().keySet()) {
            setColumn(instance, columnName, schema.getColumns().get(columnName), row);
        }
        
        for (Map.Entry<Field, Schema> entry : schema.getOneToManys().entrySet()) {
            
            Schema s = entry.getValue();
            Collection<Object> relations = Lists.newArrayList();
            String joinTable = joinTableName(schema.getTableName(), s.getTableName());
            Statement statement = select().from(joinTable).where(eq(joinColumnName(schema.getTableName()), id));
            statement.setConsistencyLevel(getDriverConsistencyLevel(consistency));

            for (Row r : executeStatement(statement, consistency)) {
                UUID u = r.getUUID(joinColumnName(s.getTableName()));

                Optional<?> joined = read(s.getObjectType(), u);

                if (joined.isPresent()) {
                    relations.add(joined.get());
                }
                else {
                    LOG.debug("Lookup for relation with ID {} failed, (skipping)", u);
                }

            }

            Util.setFieldValue(entry.getKey(), instance, relations);

        }

        cacheInstance(instance);

        return Optional.of(instance);
    }

    @Override
    public <T> Optional<T> read(Class<T> cls, String indexedName, Object value) {
        return read(cls, indexedName, value, m_consistency);
    }

    @Override
    public <T> Optional<T> read(Class<T> cls, String indexedName, Object value, ConsistencyLevel consistency) {

        checkNotNull(cls, "class argument");
        checkNotNull(indexedName, "indexedName argument");
        checkNotNull(value, "value argument");
        checkNotNull(consistency, "consistency level argument");
        checkState(m_isOpen, format("%s is closed", getClass().getSimpleName()));

        T instance = Util.newInstance(cls);

        Schema schema = getSchema(instance);

        if (!schema.isIndexed(indexedName)) {
            throw new UnsupportedOperationException(format("unindexed or non-existent column '%s'", indexedName));
        }

        Statement selectStatement = select(joinColumnName(schema.getTableName())).from(indexTableName(schema.getTableName(), indexedName)).where(eq(indexedName, value));
        selectStatement.setConsistencyLevel(getDriverConsistencyLevel(consistency));
        ResultSet results = executeStatement(selectStatement, consistency);
        Row row = results.one();

        checkState(results.isExhausted(), "query returned more than one row");
        if (row == null) return Optional.absent();

        return read(cls, row.getUUID(joinColumnName(schema.getTableName())), consistency);
    }

    private <T> void cacheInstance(T inst) {
        Schema schema = getSchema(inst);
        Record record = new Record(schema.getIDValue(inst));

        for (String columnName : schema.getColumns().keySet()) {
            record.putColumn(columnName, schema.getColumnValue(columnName, inst));
        }

        for (Field f : schema.getOneToManys().keySet()) {
            Collection<?> relations = (Collection<?>) Util.getFieldValue(f, inst);
            record.putOneToMany(f, (relations != null) ? Lists.newArrayList(relations) : null);
        }

        m_instanceCache.put(getInstanceID(inst), record);

    }

    private void setColumn(Object obj, String name, Field f, Row data) {

        try {
            if (f.getType().equals(Boolean.TYPE)) {
                f.set(obj, data.getBool(name));
            }
            else if (f.getType().equals(BigDecimal.class)) {
                f.set(obj, data.getDecimal(name));
            }
            else if (f.getType().equals(BigInteger.class)) {
                f.set(obj, data.getVarint(name));
            }
            else if (f.getType().equals(Date.class)) {
                f.set(obj, data.getDate(name));
            }
            else if (f.getType().equals(Double.TYPE)) {
                f.set(obj, data.getDouble(name));
            }
            else if (f.getType().equals(Float.TYPE)) {
                f.set(obj, data.getFloat(name));
            }
            else if (f.getType().equals(InetAddress.class)) {
                f.set(obj, data.getInet(name));
            }
            else if (f.getType().equals(Integer.TYPE)) {
                f.set(obj, data.getInt(name));
            }
            else if (f.getType().equals(List.class)) {
                // FIXME: ...
                throw new UnsupportedOperationException();
            }
            else if (f.getType().equals(Long.TYPE)) {
                f.set(obj, data.getLong(name));
            }
            else if (f.getType().equals(Map.class)) {
                // FIXME: ...
                throw new UnsupportedOperationException();
            }
            else if (f.getType().equals(Set.class)) {
                // FIXME: ...
                throw new UnsupportedOperationException();
            }
            else if (f.getType().equals(String.class)) {
                f.set(obj, data.getString(name));
            }
            else if (f.getType().equals(UUID.class)) {
                f.set(obj, data.getUUID(name));
            }
            else {
                throw new IllegalArgumentException(format("Unsupported field type %s", f.getType()));
            }
        }
        catch (IllegalArgumentException | IllegalAccessException e) {
            throw propagate(e);
        }

    }

    @Override
    public <T> void delete(T obj) {
        delete(obj, m_consistency);
    }

    @Override
    public <T> void delete(T obj, ConsistencyLevel consistency) {

        checkNotNull(obj, "object argument");
        checkNotNull(consistency, "consistency level argument");
        checkState(m_isOpen, format("%s is closed", getClass().getSimpleName()));

        Schema schema = getSchema(obj);
        Batch batchStatement = batch(QueryBuilder.delete().from(schema.getTableName())
                .where(eq(schema.getIDName(), schema.getIDValue(obj))));

        // Remove index entries
        for (Entry<String, Field> entry : schema.getColumns().entrySet()) {
            String columnName = entry.getKey();
            Field f = entry.getValue();

            if (f.isAnnotationPresent(INDEX)) {
                String tableName = indexTableName(schema.getTableName(), columnName);
                batchStatement.add(QueryBuilder.delete().from(tableName).where(eq(columnName, Util.getFieldValue(f, obj))));
            }
        }

        // Remove one-to-many relationships
        for (Schema s : schema.getOneToManys().values()) {
            String joinTable = joinTableName(schema.getTableName(), s.getTableName());
            batchStatement.add(
                    QueryBuilder.delete().from(joinTable)
                        .where(eq(joinColumnName(schema.getTableName()), schema.getIDValue(obj)))
            );
        }

        executeStatement(batchStatement, consistency);

        m_instanceCache.remove(getInstanceID(obj));

    }

    @Override
    public void close() throws IOException {
        m_isOpen = false;
    }

    private ResultSet executeStatement(Statement statement, ConsistencyLevel cl) {
        try {
            statement.setConsistencyLevel(getDriverConsistencyLevel(cl));
            return m_session.execute(statement);
        }
        catch (DriverException driverExcp) {
            throw new LucidityException(driverExcp);
        }
    }

}
