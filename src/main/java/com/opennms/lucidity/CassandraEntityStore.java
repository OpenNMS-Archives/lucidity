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

import static com.datastax.driver.core.querybuilder.QueryBuilder.addAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.removeAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.opennms.lucidity.Schema.ENTITY;
import static com.opennms.lucidity.Schema.ID;
import static com.opennms.lucidity.Schema.indexTableName;
import static com.opennms.lucidity.Schema.joinColumnName;
import static com.opennms.lucidity.Schema.joinTableName;
import static java.lang.String.format;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
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

import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.opennms.lucidity.Schema.ColumnSpec;
import com.opennms.lucidity.annotations.UpdateStrategy;


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
                schema.getID().getValue(object) == null,
                format("property annotated with @%s must be null", ID.getCanonicalName()));

        // Object persistence (incl. indices)
        UUID id = UUID.randomUUID();
        Batch batch = batch();
        Insert insertStatement = insertInto(schema.getTableName()).value(schema.getID().getName(), id);

        for (ColumnSpec colSpec : schema.getColumns()) {
            
            insertStatement.value(colSpec.getName(), colSpec.getValue(object));
            
            if (colSpec.isIndexed()) {
                String tableName = indexTableName(schema.getTableName(), colSpec.getName());
                batch.add(
                        insertInto(tableName)
                            .value(colSpec.getName(), colSpec.getValue(object))
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
                UUID relationID = (UUID) s.getID().getValue(item);

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

        schema.getID().setValue(object, id);
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

        // Begin with standard (i.e. non-collection) columns.
        for (ColumnSpec colSpec : schema.getStandardColumns()) {

            Object past, current;
            current = colSpec.getValue(object);
            past = record.getColumns().get(colSpec.getName());

            if (current != null && !current.equals(past)) {
                needsUpdate = true;
                updateStatement.with(set(colSpec.getName(), current));

                // Update index, if applicable
                if (colSpec.isIndexed()) {
                    batchStatement.add(
                            QueryBuilder.update(indexTableName(schema.getTableName(), colSpec.getName()))
                                    .with(set(joinColumnName(schema.getTableName()), schema.getID().getValue(object)))
                                    .where(eq(colSpec.getName(), colSpec.getValue(object)))
                    );
                }
            }
        }

        updateStatement.where(eq(schema.getID().getName(), schema.getID().getValue(object)));

        if (needsUpdate) {
            batchStatement.add(updateStatement);
        }

        // Next, collection columns ...
        for (ColumnSpec colSpec : schema.getCollectionColumns()) {

            Object past, current;
            current = colSpec.getValue(object);
            past = record.getColumns().get(colSpec.getName());

            if (current != null && !current.equals(past)) {

                if (colSpec.getCollectionUpdateStrategy().equals(UpdateStrategy.ELEMENT)) {
                    Collection<RegularStatement> statements = diffCollection(
                            schema.getTableName(),
                            colSpec.getName(),
                            eq(schema.getID().getName(), schema.getID().getValue(object)),
                            past,
                            current);
                    for (RegularStatement statement : statements) {
                        batchStatement.add(statement);
                    }
                }
                else {
                    batchStatement.add(
                            insertInto(schema.getTableName())
                                .value(colSpec.getName(), current)
                                .value(schema.getID().getName(), schema.getID().getValue(object))
                    );
                }

            }

        }

        // Finally, process one-to-many mappings
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
                if (s.getID().getValue(o) == null) {
                    throw new IllegalStateException(
                            "encountered relation with null ID property (entity not persisted?)");
                }
                batchStatement.add(insertInto(joinTable)
                        .value(joinColumnName(schema.getTableName()), schema.getID().getValue(object))
                        .value(joinColumnName(s.getTableName()), s.getID().getValue(o)));
            }

            for (Object o : toRemove) {
                if (s.getID().getValue(o) == null) {
                    throw new IllegalStateException(
                            "encountered relation with null ID property (entity not persisted?)");
                }
                batchStatement.add(
                        QueryBuilder.delete().from(joinTable)
                            .where(eq(joinColumnName(schema.getTableName()), schema.getID().getValue(object)))
                                .and(eq(joinColumnName(s.getTableName()), s.getID().getValue(o)))
                );
            }
        }

        executeStatement(batchStatement, consistency);
        cacheInstance(object);

    }

    private Collection<RegularStatement> diffCollection(String table, String column, Clause whereClause, Object past, Object present) {
        if (past instanceof Set<?>) {
            return diffSet(table, column, whereClause, (Set<?>) past, (Set<?>) present);
        }
        else if (past instanceof Map<?, ?>) {
            return diffMap(table, column, whereClause, (Map<?, ?>) past, (Map<?, ?>) present);
        }
        else {
            throw new RuntimeException("unknown collection type!");
        }
    }

    private Collection<RegularStatement> diffSet(String table, String column, Clause whereClause, Set<?> past,
            Set<?> present) {

        List<RegularStatement> queries = Lists.newArrayList();

        Set<?> removes = Sets.newHashSet(past);
        removes.removeAll(present);

        if (!removes.isEmpty()) {
            queries.add(QueryBuilder.update(table).with(removeAll(column, removes)).where(whereClause));
        }

        Set<?> adds = Sets.newHashSet(present);
        adds.removeAll(past);

        if (!adds.isEmpty()) {
            queries.add(QueryBuilder.update(table).with(addAll(column, adds)).where(whereClause));
        }

        return queries;
    }

    private Collection<RegularStatement> diffMap(String table, String column, Clause whereClause, Map<?, ?> past,
            Map<?, ?> present) {

        List<RegularStatement> queries = Lists.newArrayList();

        Set<?> removed = Sets.newHashSet(past.keySet());
        removed.removeAll(present.keySet());

        if (!removed.isEmpty()) {
            Delete.Selection delete = QueryBuilder.delete();

            for (Object o : removed) {
                delete.mapElt(column, o);
            }

            queries.add(delete.from(table).where(whereClause));
        }

        Set<Entry<?, ?>> changed = Sets.<Entry<?, ?>> newHashSet(present.entrySet());
        changed.removeAll(past.entrySet());

        if (!changed.isEmpty()) {
            Update update = QueryBuilder.update(table);

            for (Entry<?, ?> entry : changed) {
                update.with(QueryBuilder.put(column, entry.getKey(), entry.getValue()));
            }

            queries.add(update.where(whereClause));
        }

        return queries;
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
        Statement selectStatement = select().from(schema.getTableName()).where(eq(schema.getID().getName(), id));
        selectStatement.setConsistencyLevel(getDriverConsistencyLevel(consistency));
        ResultSet results = executeStatement(selectStatement, consistency);
        Row row = results.one();

        checkState(results.isExhausted(), "query returned more than one row");
        if (row == null) return Optional.absent();

        schema.getID().setValue(instance, row.getUUID(schema.getID().getName()));

        for (ColumnSpec colSpec : schema.getColumns()) {
            setColumn(instance, colSpec, row);
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
        Record record = new Record(schema.getID().getValue(inst));

        for (ColumnSpec colSpec : schema.getColumns()) {
            record.putColumn(colSpec.getName(), copyOf(colSpec.getValue(inst)));
        }

        for (Field f : schema.getOneToManys().keySet()) {
            Collection<?> relations = (Collection<?>) Util.getFieldValue(f, inst);
            record.putOneToMany(f, (relations != null) ? Lists.newArrayList(relations) : null);
        }

        m_instanceCache.put(getInstanceID(inst), record);

    }

    private Object copyOf(Object obj) {
        if (obj instanceof Map) {
            return Maps.newHashMap((Map<?, ?>)obj);
        }
        else if (obj instanceof Set) {
            return Sets.newHashSet((Set<?>)obj);
        }
        else if (obj instanceof List) {
            return Lists.newArrayList((List<?>)obj);
        }
        else {
            return obj;
        }
    }

    private void setColumn(Object obj, ColumnSpec colSpec, Row data) {

        if (colSpec.getType().equals(Boolean.TYPE)) {
            colSpec.setValue(obj, data.getBool(colSpec.getName()));
        }
        else if (colSpec.getType().equals(BigDecimal.class)) {
            colSpec.setValue(obj, data.getDecimal(colSpec.getName()));
        }
        else if (colSpec.getType().equals(BigInteger.class)) {
            colSpec.setValue(obj, data.getVarint(colSpec.getName()));
        }
        else if (colSpec.getType().equals(Date.class)) {
            colSpec.setValue(obj, data.getDate(colSpec.getName()));
        }
        else if (colSpec.getType().equals(Double.TYPE)) {
            colSpec.setValue(obj, data.getDouble(colSpec.getName()));
        }
        else if (colSpec.getType().equals(Float.TYPE)) {
            colSpec.setValue(obj, data.getFloat(colSpec.getName()));
        }
        else if (colSpec.getType().equals(InetAddress.class)) {
            colSpec.setValue(obj, data.getInet(colSpec.getName()));
        }
        else if (colSpec.getType().equals(Integer.TYPE)) {
            colSpec.setValue(obj, data.getInt(colSpec.getName()));
        }
        else if (colSpec.getType().equals(List.class)) {
            colSpec.setValue(obj, data.getList(colSpec.getName(), (Class<?>) colSpec.getParameterizedTypes()[0]));
        }
        else if (colSpec.getType().equals(Long.TYPE)) {
            colSpec.setValue(obj, data.getLong(colSpec.getName()));
        }
        else if (colSpec.getType().equals(Map.class)) {
            Type[] types = colSpec.getParameterizedTypes();
            colSpec.setValue(obj, data.getMap(colSpec.getName(), (Class<?>) types[0], (Class<?>) types[1]));
        }
        else if (colSpec.getType().equals(Set.class)) {
            colSpec.setValue(obj, data.getSet(colSpec.getName(), (Class<?>) colSpec.getParameterizedTypes()[0]));
        }
        else if (colSpec.getType().equals(String.class)) {
            colSpec.setValue(obj, data.getString(colSpec.getName()));
        }
        else if (colSpec.getType().equals(UUID.class)) {
            colSpec.setValue(obj, data.getUUID(colSpec.getName()));
        }
        else {
            throw new IllegalArgumentException(format("Unsupported field type %s", colSpec.getType()));
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
                .where(eq(schema.getID().getName(), schema.getID().getValue(obj))));

        // Remove index entries
        for (ColumnSpec colSpec : schema.getColumns()) {
            if (colSpec.isIndexed()) {
                String tableName = indexTableName(schema.getTableName(), colSpec.getName());
                batchStatement.add(QueryBuilder.delete().from(tableName).where(eq(colSpec.getName(), colSpec.getValue(obj))));
            }
        }

        // Remove one-to-many relationships
        for (Schema s : schema.getOneToManys().values()) {
            String joinTable = joinTableName(schema.getTableName(), s.getTableName());
            batchStatement.add(
                    QueryBuilder.delete().from(joinTable)
                        .where(eq(joinColumnName(schema.getTableName()), schema.getID().getValue(obj)))
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
