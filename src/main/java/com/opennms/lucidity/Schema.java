package com.opennms.lucidity;


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
import java.util.Map;
import java.util.UUID;

import com.google.common.base.Throwables;
import com.google.common.collect.Maps;
import com.opennms.lucidity.annotations.Column;
import com.opennms.lucidity.annotations.Entity;
import com.opennms.lucidity.annotations.Id;
import com.opennms.lucidity.annotations.Index;
import com.opennms.lucidity.annotations.OneToMany;
import com.opennms.lucidity.annotations.Table;

// FIXME: support annotated methods, as well as fields.
// FIXME: add collection types to CQL_TYPES.

// ~~~

//FIXME: column names should default to property name when name is not set.
//FIXME: fromObject should validate field types against CQL_TYPES.


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
    
    static final String DEFAULT_ID_NAME = "id";
    static final Map<Class<?>, String> CQL_TYPES = Maps.newHashMap();
    
    static {
        CQL_TYPES.put(Boolean.TYPE, "boolean");
        CQL_TYPES.put(BigDecimal.class, "decimal");
        CQL_TYPES.put(BigInteger.class, "varint");
        CQL_TYPES.put(Date.class, "timestamp");
        CQL_TYPES.put(Double.TYPE, "double");
        CQL_TYPES.put(Float.TYPE, "float");
        CQL_TYPES.put(InetAddress.class, "inet");
        CQL_TYPES.put(Integer.TYPE, "int");
        CQL_TYPES.put(Long.TYPE, "bigint");
        CQL_TYPES.put(String.class, "text");
        CQL_TYPES.put(UUID.class, "uuid");
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

    String toDDL() {

        StringBuilder sb = new StringBuilder();

        sb.append("CREATE TABLE ").append(getTableName()).append(" (").append(getIDName()).append(" uuid PRIMARY KEY");

        for (Map.Entry<String, Field> entry : getColumns().entrySet()) {
            sb.append(", ").append(entry.getKey()).append(" ").append(CQL_TYPES.get(entry.getValue().getType()));
        }

        sb.append(");").append(System.lineSeparator());
        
        for (Map.Entry<String, Field> entry : getColumns().entrySet()) {
            String columnName = entry.getKey();
            
            if (entry.getValue().isAnnotationPresent(INDEX)) {
                sb.append(format(
                        "CREATE TABLE %s_%s_idx (%s %s PRIMARY KEY, %s_id uuid);%n",
                        getTableName(),
                        columnName,
                        columnName,
                        CQL_TYPES.get(entry.getValue().getType()),
                        getTableName()));
            }
        }

        for (Schema s : getOneToManys().values()) {
            sb.append(format(
                    "CREATE TABLE %s_%s (%s_id uuid, %s_id uuid, PRIMARY KEY(%s_id, %s_id));%n",
                    getTableName(),
                    s.getTableName(),
                    getTableName(),
                    s.getTableName(),
                    getTableName(),
                    s.getTableName()));
        }

        return sb.toString();
    }

    @Override
    public String toString() {
        return format("%s[tableName=%s]", getClass().getSimpleName(), getTableName());
    }

    static Schema fromClass(Class<?> cls) {
        checkNotNull(cls, "object reference argument");

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
                
                if (f.isAnnotationPresent(COLUMN)) {
                    Column c = f.getAnnotation(Column.class);
                    idName = c.name();
                }
                else {
                    idName = DEFAULT_ID_NAME;
                }

                if (!f.getType().equals(UUID.class)) {
                    throw new IllegalArgumentException(format("@%s must be of type UUID", ID.getCanonicalName()));
                }

                idField = f;

            }
            // Column annotated fields
            else if (f.isAnnotationPresent(COLUMN)) {
                f.setAccessible(true);
                Column c = f.getAnnotation(Column.class);
                
                try {
                    f.setAccessible(true);
                    // FIXME sw: check for empty name
                    columns.put(c.name(), f);
                }
                catch (IllegalArgumentException e) {
                    throw Throwables.propagate(e);
                }
            }
            // OnToMany annotated fields
            else if (f.isAnnotationPresent(ONE_TO_MANY)) {
                f.setAccessible(true);

                if (!f.getType().equals(Collection.class)) {
                    throw new IllegalArgumentException(
                            format("Fields annotated with @%s must be of type Collection", ONE_TO_MANY.getCanonicalName()));
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

}
