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


import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Predicate;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.opennms.lucidity.annotations.Column;
import com.opennms.lucidity.annotations.EmbeddedCollection;
import com.opennms.lucidity.annotations.Entity;
import com.opennms.lucidity.annotations.Id;
import com.opennms.lucidity.annotations.Index;
import com.opennms.lucidity.annotations.OneToMany;
import com.opennms.lucidity.annotations.Table;
import com.opennms.lucidity.annotations.UpdateStrategy;

// FIXME: support annotated methods, as well as fields.
// FIXME: add collection types to CQL_TYPES.

/**
 * Groks schema structure from an annotated Java object.
 * 
 * @author eevans
 */
class Schema {

    static final Class<? extends Annotation> ENTITY = Entity.class;
    static final Class<? extends Annotation> COLUMN = Column.class;
    static final Class<? extends Annotation> ID = Id.class;
    static final Class<? extends Annotation> ONE_TO_MANY = OneToMany.class;
    static final Class<? extends Annotation> INDEX = Index.class;
    static final Class<? extends Annotation> TABLE = Table.class;
    static final Class<? extends Annotation> COLLECTION = EmbeddedCollection.class;
    
    static final String DEFAULT_ID_NAME = "id";
    static final Set<Class<?>> COLLECTION_TYPES = Sets.<Class<?>>newHashSet(Map.class, Set.class, List.class);
    static final Map<Type, String> CQL_TYPES = Maps.newHashMap();

    static {
        CQL_TYPES.put(Boolean.TYPE, "boolean");
        CQL_TYPES.put(Boolean.class, "boolean");
        CQL_TYPES.put(BigDecimal.class, "decimal");
        CQL_TYPES.put(BigInteger.class, "varint");
        CQL_TYPES.put(Date.class, "timestamp");
        CQL_TYPES.put(Double.TYPE, "double");
        CQL_TYPES.put(Double.class, "double");
        CQL_TYPES.put(Float.TYPE, "float");
        CQL_TYPES.put(Float.class, "float");
        CQL_TYPES.put(InetAddress.class, "inet");
        CQL_TYPES.put(Integer.TYPE, "int");
        CQL_TYPES.put(Integer.class, "int");
        CQL_TYPES.put(Long.TYPE, "bigint");
        CQL_TYPES.put(Long.class, "bigint");
        CQL_TYPES.put(String.class, "text");
        CQL_TYPES.put(UUID.class, "uuid");
        CQL_TYPES.put(Map.class, "map");
        CQL_TYPES.put(Set.class, "set");
        CQL_TYPES.put(List.class, "list");
    }

    private final Class<?> m_type;
    private final String m_tableName;
    private final String m_idName;
    private final Field m_idField;
    private final Map<String, Field> m_columns;
    private final Map<Field, Schema> m_oneToManys;

    Schema(Class<?> type, String tableName, String idName, Field idField, Map<String, Field> columns, Map<Field, Schema> oneToManys) {
        m_type = type;
        m_tableName = tableName;
        m_idName = idName;
        m_idField = idField;
        m_columns = columns;
        m_oneToManys = oneToManys;
    }

    Class<?> getObjectType() {
        return m_type;
    }

    String getTableName() {
        return m_tableName;
    }

    String getIDName() {
        return m_idName;
    }

    Field getIDField() {
        return m_idField;
    }

    UUID getIDValue(Object obj) {
        return (UUID)Util.getFieldValue(m_idField, obj);
    }

    Map<String, Field> getColumns() {
        return m_columns;
    }

    Map<Field, Schema> getOneToManys() {
        return m_oneToManys;
    }

    Object getColumnValue(String columnName, Object obj) {
        return Util.getFieldValue(m_columns.get(columnName), obj);
    }

    boolean isIndexed(String columnName) {
        if (!getColumns().containsKey(columnName)) {
            return false;
        }
        return getColumns().get(columnName).isAnnotationPresent(INDEX);
    }

    /** Return all standard (read: non-collection) columns */
    Map<String, Field> getStandardColumns() {
        return Maps.filterValues(getColumns(), new Predicate<Field>() {

            @Override
            public boolean apply(Field input) {
                return !input.isAnnotationPresent(COLLECTION);
            }
        });
    }

    /** Return only collection columns (maps, lists, and sets). */
    Map<String, Field> getCollectionColumns() {
        return Maps.filterValues(getColumns(), new Predicate<Field>() {

            @Override
            public boolean apply(Field input) {
                return input.isAnnotationPresent(COLLECTION);
            }
        });
    }

    String toDDL() {

        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ").append(getTableName()).append(" (").append(getIDName()).append(" uuid PRIMARY KEY");

        for (Map.Entry<String, Field> entry : getColumns().entrySet()) {
            sb.append(", ").append(entry.getKey()).append(" ").append(getCassandraTypeDDL(entry.getValue()));
        }

        sb.append(");").append(System.lineSeparator());
        
        for (Map.Entry<String, Field> entry : getColumns().entrySet()) {
            String columnName = entry.getKey();
            
            if (entry.getValue().isAnnotationPresent(INDEX)) {
                sb.append(format(
                        "CREATE TABLE %s (%s %s PRIMARY KEY, %s uuid);%n",
                        indexTableName(getTableName(), columnName),
                        columnName,
                        getCassandraTypeDDL(entry.getValue()),
                        joinColumnName(getTableName())));
            }
        }

        for (Schema s : getOneToManys().values()) {
            sb.append(format(
                    "CREATE TABLE %s (%s uuid, %s uuid, PRIMARY KEY(%s, %s));%n",
                    joinTableName(getTableName(), s.getTableName()),
                    joinColumnName(getTableName()),
                    joinColumnName(s.getTableName()),
                    joinColumnName(getTableName()),
                    joinColumnName(s.getTableName())));
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return format("%s[tableName=%s]", getClass().getSimpleName(), getTableName());
    }

    static Schema fromClass(Class<?> cls) {
        checkNotNull(cls, "class argument");

        if (!Util.getNoArgConstructor(cls).isPresent()) {
            throw new IllegalArgumentException(format("%s is missing nullary constructor.", cls.getCanonicalName()));
        }

        String tableName = cls.getSimpleName();

        if (cls.isAnnotationPresent(TABLE)) {
            Table table = cls.getAnnotation(Table.class);
            if (!table.name().isEmpty()) {
                tableName = table.name();
            }
        }

        String idName = null;
        Field idField = null;
        Map<String, Field> columns = Maps.newHashMap();
        Map<Field, Schema> oneToManys = Maps.newHashMap();

        // Fields
        for (Field f : cls.getDeclaredFields()) {
            
            // ID fields
            if (f.isAnnotationPresent(ID)) {
                f.setAccessible(true);
                
                idName = getColumnSchemaName(f, DEFAULT_ID_NAME);

                if (!f.getType().equals(UUID.class)) {
                    throw new IllegalArgumentException(format("@%s must be of type UUID", ID.getCanonicalName()));
                }

                idField = f;

            }
            // EmbeddedCollection annotated fields
            else if (f.isAnnotationPresent(COLLECTION)) {
                f.setAccessible(true);

                checkArgument(
                        isCassandraCollection(f.getType()),
                        format("%s is an invalid type for @%s", f.getType(), COLLECTION.getCanonicalName()));

                if (f.getType().equals(List.class)) {
                    EmbeddedCollection c = f.getAnnotation(EmbeddedCollection.class);
                    checkArgument(
                            !c.updateStrategy().equals(UpdateStrategy.ELEMENT),
                            format("unsupported update strategy %s for collection of type List", UpdateStrategy.ELEMENT));
                }

                String name = getColumnSchemaName(f);

                checkArgument(
                        !f.isAnnotationPresent(INDEX),
                        format("Cannot use @%s annotation on collection %s", INDEX.getCanonicalName(), name));

                for (Type t : Util.getParameterizedTypes(f)) {
                    checkArgument(
                            isCassandraType(t),
                            format("unsupported parameter type (%s) for collection %s", t, name));
                }

                columns.put(name, f);

            }
            // Column annotated fields
            else if (f.isAnnotationPresent(COLUMN)) {
                f.setAccessible(true);

                checkArgument(
                        !isCassandraCollection(f.getType()),
                        format("%s is invalid for standard column (missing @%s annotation?)", f.getType().getCanonicalName(), COLLECTION.getCanonicalName()));

                String name = getColumnSchemaName(f);

                checkArgument(isCassandraType(f.getType()), format("invalid type: %s (%s)", f.getType(), name));

                columns.put(name, f);

            }
            // OnToMany annotated fields
            else if (f.isAnnotationPresent(ONE_TO_MANY)) {
                f.setAccessible(true);

                if (!f.getType().equals(Collection.class)) {
                    throw new IllegalArgumentException(
                            format("Fields annotated with @%s must be of type EmbeddedCollection", ONE_TO_MANY.getCanonicalName()));
                }

                Type type = ((ParameterizedType)f.getGenericType()).getActualTypeArguments()[0];
                oneToManys.put(f, fromClass((Class<?>)type));

            }

            // FIXME sw: should log warning about unsupported annotations
            // FIXME sw: should log warning about fields without annotations (@Transient ... I don't like it)
        }

        if (idField == null) {
            throw new IllegalArgumentException(String.format("Missing @%s annotation", ID.getCanonicalName()));
        }

        if (columns.size() < 1) {
            throw new IllegalArgumentException(
                    String.format("At least one non-Id field must be annotated with @%s", COLUMN.getCanonicalName()));
        }

        return new Schema(cls, tableName, idName, idField, columns, oneToManys);
    }

    static String joinColumnName(String tableName) {
        return format("%s_id", tableName);
    }

    static String indexTableName(String tableName, String indexedName) {
        return format("%s_%s_idx", tableName, indexedName);
    }

    static String joinTableName(String table0, String table1) {
        return format("%s_%s", table0, table1);
    }

    private static boolean isCassandraCollection(Type type) {
        return COLLECTION_TYPES.contains(type);
    }

    private static String getCassandraTypeDDL(Field f) {

        Type type = f.getType();

        if (type.equals(Map.class)) {
            Type[] t = Util.getParameterizedTypes(f);
            return format("%s<%s, %s>", CQL_TYPES.get(type), CQL_TYPES.get(t[0]), CQL_TYPES.get(t[1]));
        }
        else if (type.equals(Set.class) || type.equals(List.class)) {
            return format("%s<%s>", CQL_TYPES.get(type), CQL_TYPES.get(Util.getParameterizedTypes(f)[0]));
        }
        else {
            return CQL_TYPES.get(type);
        }

    }

    private static boolean isCassandraType(Type type) {
        return CQL_TYPES.containsKey(type);
    }

    private static String getColumnSchemaName(Field f) {
        return getColumnSchemaName(f, f.getName());
    }

    private static String getColumnSchemaName(Field f, String def) {
        
        String name = def;
        
        if (f.isAnnotationPresent(COLUMN)) {
            Column c = f.getAnnotation(Column.class);
            if (!c.name().equals("")) name = c.name();
        }
        
        return name;
    }

}
