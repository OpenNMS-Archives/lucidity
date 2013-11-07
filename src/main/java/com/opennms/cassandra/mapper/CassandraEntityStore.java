package com.opennms.cassandra.mapper;


import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.propagate;
import static com.opennms.cassandra.mapper.Schema.ENTITY;
import static com.opennms.cassandra.mapper.Schema.ID;
import static com.opennms.cassandra.mapper.Schema.INDEX;
import static com.opennms.cassandra.mapper.Schema.joinColumnName;
import static java.lang.String.format;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;


// FIXME: Consider something from Guava for cache.
// FIXME: Wrap java driver exceptions in something (don't expose to consumers of this API).
// FIXME: Reflection errors shouldn't be propagated as RuntimeExceptions (use custom exception).
// FIXME: Cache schemas?
// FIXME: Support collection types.
// FIXME: delete() should remove from instance cache as well.
// FIXME: replace instances of Class.newInstance() with method from Util

public class CassandraEntityStore implements EntityStore {

    private final com.datastax.driver.core.Session m_session;
    private ConcurrentMap<UUID, Record> m_objectCache = new ConcurrentHashMap<>();

    public CassandraEntityStore(String host, int port, String keyspace) {

        checkNotNull(host, "Cassandra hostname");
        checkNotNull(port, "Cassandra port number");
        checkNotNull(keyspace, "Cassandra keyspace");

        Cluster cluster = Cluster.builder().withPort(port).addContactPoint(host).build();
        m_session = cluster.connect(keyspace);

    }

    private Schema getSchema(Object object) {
        return Schema.fromClass(object.getClass());
    }

    private com.datastax.driver.core.ConsistencyLevel getSessionConsistencyLevel(Session<?> session) {
        return getDriverConsistencyLevel(session.getConsistencyLevel());
    }
    
    private com.datastax.driver.core.ConsistencyLevel getDriverConsistencyLevel(ConsistencyLevel cl) {
        return com.datastax.driver.core.ConsistencyLevel.fromCode(cl.getDriverCode());
    }

    @Override
    public <T> Session<T> create(T object) {
        return create(object, Session.DEFAULT_CONSISTENCY_LEVEL);
    }
    
    @Override
    public <T> Session<T> create(T object, ConsistencyLevel consistency) {

        checkNotNull(object, "object argument");
        checkArgument(
                object.getClass().isAnnotationPresent(ENTITY),
                String.format("%s not annotated with @%s", getClass().getSimpleName(), ENTITY.getCanonicalName()));

        Schema schema = getSchema(object);
        
        checkArgument(
                schema.getIDValue(object) == null,
                String.format("property annotated with @%s must be null", ID.getCanonicalName()));

        // Object persistence (incl. indices)
        UUID id = UUID.randomUUID();
        Batch batch = batch();
        Insert insertStatement = insertInto(schema.getTableName()).value(schema.getIDName(), id);

        for (Entry<String, Field> entry : schema.getColumns().entrySet()) {
            String columnName = entry.getKey();
            Field f = entry.getValue();
            
            insertStatement.value(columnName, schema.getColumnValue(columnName, object));
            
            if (entry.getValue().isAnnotationPresent(Schema.INDEX)) {
                String tableName = format("%s_%s_idx", schema.getTableName(), columnName);
                batch.add(
                        insertInto(tableName)
                            .value(columnName, Util.getFieldValue(f, object))
                            .value(format("%s_id", schema.getTableName()), id)
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

                String joinTable = format("%s_%s", schema.getTableName(), s.getTableName());

                batch.add(
                        insertInto(joinTable)
                            .value(joinColumnName(schema.getTableName()), id)
                            .value(joinColumnName(s.getTableName()), relationID)
                );
            }

        }

        batch.setConsistencyLevel(getDriverConsistencyLevel(consistency));
        m_session.execute(batch);

        Util.setFieldValue(schema.getIDField(), object, id);
        
        return cacheSession(new Session<T>(object).setConsistencyLevel(consistency));
    }

    @Override
    public <T> void update(Session<T> session) {

        Record record = m_objectCache.get(session.getID());

        if (record == null) {
            throw new IllegalStateException("session is invalid");
        }

        Object object = session.get();

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
                            QueryBuilder.update(format("%s_%s_idx", schema.getTableName(), columnName))
                                    .with(set(format("%s_id", schema.getTableName()), schema.getIDValue(object)))
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

            String joinTable = format("%s_%s", schema.getTableName(), s.getTableName());

            for (Object o : toInsert) {
                if (s.getIDValue(o) == null) {
                    throw new IllegalStateException(
                            "encountered relation with null ID property (entity not persisted?)");
                }
                batchStatement.add(insertInto(joinTable).value(
                        joinColumnName(schema.getTableName()),
                        schema.getIDValue(object)).value(joinColumnName(s.getTableName()), s.getIDValue(o)));
            }

            for (Object o : toRemove) {
                if (s.getIDValue(o) == null) {
                    throw new IllegalStateException(
                            "encountered relation with null ID property (entity not persisted?)");
                }
                batchStatement.add(QueryBuilder.delete().from(joinTable).where(
                        eq(joinColumnName(schema.getTableName()), schema.getIDValue(object))).and(
                        eq(joinColumnName(s.getTableName()), s.getIDValue(o))));
            }
        }

        batchStatement.setConsistencyLevel(getSessionConsistencyLevel(session));
        m_session.execute(batchStatement);

    }

    @Override
    public <T> Session<T> read(Class<T> cls, UUID id) {
        return read(cls, id, Session.DEFAULT_CONSISTENCY_LEVEL);
    }
    
    @Override
    public <T> Session<T> read(Class<T> cls, UUID id, ConsistencyLevel consistency) {

        T instance;
        try {
            instance = cls.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw propagate(e);    // Missing ctor?
        }

        Schema schema = getSchema(instance);
        Statement selectStatement = select().from(schema.getTableName()).where(eq(schema.getIDName(), id));
        selectStatement.setConsistencyLevel(getDriverConsistencyLevel(consistency));
        ResultSet results = m_session.execute(selectStatement);
        Row row = results.one();

        checkState(results.isExhausted(), "query returned more than one row");
        if (row == null) return new Session<T>(null);

        Util.setFieldValue(schema.getIDField(), instance, row.getUUID(schema.getIDName()));

        for (String columnName : schema.getColumns().keySet()) {
            setColumn(instance, columnName, schema.getColumns().get(columnName), row);
        }
        
        for (Map.Entry<Field, Schema> entry : schema.getOneToManys().entrySet()) {
            
            Schema s = entry.getValue();
            Collection<Object> relations = Lists.newArrayList();
            String joinTable = format("%s_%s", schema.getTableName(), s.getTableName());
            Statement statement = select().from(joinTable).where(eq(joinColumnName(schema.getTableName()), id));
            statement.setConsistencyLevel(getDriverConsistencyLevel(consistency));

            for (Row r : m_session.execute(statement)) {
                UUID u = r.getUUID(joinColumnName(s.getTableName()));

                Session<?> joined = read(s.getObjectType(), u);

                // XXX: This will silently ignore negative hits, is that what we want?
                if (joined.get() != null) {
                    relations.add(read(s.getObjectType(), u).get());
                }
            }

            Util.setFieldValue(entry.getKey(), instance, relations);

        }

        return cacheSession(new Session<T>(instance).setConsistencyLevel(consistency));
    }

    private <T> Session<T> cacheSession(Session<T> sess) {
        T object = sess.get();
        Schema schema = getSchema(sess.get());

        Record record = new Record(schema.getIDValue(object));
        for (String columnName : schema.getColumns().keySet()) {
            record.putColumn(columnName, schema.getColumnValue(columnName, object));
        }
        for (Field f : schema.getOneToManys().keySet()) {
            Collection<?> relations = (Collection<?>) Util.getFieldValue(f, object);
            record.putOneToMany(f, (relations != null) ? Lists.newArrayList(relations) : null);
        }
        m_objectCache.put(sess.getID(), record);
        
        return sess;
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
    public <T> void delete(Session<T> session) {

        T obj = session.get();
        
        Schema schema = getSchema(obj);
        Batch batchStatement = batch(QueryBuilder.delete().from(schema.getTableName())
                .where(eq(schema.getIDName(), schema.getIDValue(obj))));

        // Remove index entries
        for (Entry<String, Field> entry : schema.getColumns().entrySet()) {
            String columnName = entry.getKey();
            Field f = entry.getValue();

            if (f.isAnnotationPresent(INDEX)) {
                String tableName = format("%s_%s_idx", schema.getTableName(), columnName);
                batchStatement.add(QueryBuilder.delete().from(tableName).where(eq(columnName, Util.getFieldValue(f, obj))));
            }
        }

        // Remove one-to-many relationships
        for (Schema s : schema.getOneToManys().values()) {
            String joinTable = format("%s_%s", schema.getTableName(), s.getTableName());
            batchStatement.add(QueryBuilder.delete().from(joinTable)
                    .where(eq(joinColumnName(schema.getTableName()), schema.getIDValue(obj))));
        }

        batchStatement.setConsistencyLevel(getSessionConsistencyLevel(session));
        m_session.execute(batchStatement);

    }

    @Override
    public <T> Session<T> read(Class<T> cls, String indexedName, Object value) {
        return read(cls, indexedName, value, Session.DEFAULT_CONSISTENCY_LEVEL);
    }
    
    @Override
    public <T> Session<T> read(Class<T> cls, String indexedName, Object value, ConsistencyLevel consistency) {

        T instance;
        try {
            instance = cls.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw propagate(e);    // Missing ctor?
        }

        Schema schema = getSchema(instance);
        Statement selectStatement = select(format("%s_id", schema.getTableName())).from(format("%s_%s_idx", schema.getTableName(), indexedName)).where(eq(indexedName, value));
        selectStatement.setConsistencyLevel(getDriverConsistencyLevel(consistency));
        ResultSet results = m_session.execute(selectStatement);
        Row row = results.one();

        checkState(results.isExhausted(), "query returned more than one row");
        if (row == null) return new Session<T>(null);
        
        return read(cls, row.getUUID(format("%s_id", schema.getTableName())), consistency);
    }

}
