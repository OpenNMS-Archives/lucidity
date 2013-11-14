package com.opennms.lucidity;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;


/**
 * Apache Cassandra implementation of {@link EntityStoreFactory}.
 * 
 * @author eevans
 */
@Singleton
public class CassandraEntityStoreFactory implements EntityStoreFactory {

    private final Session m_session;
    private final ConsistencyLevel m_consistency;

    /**
     * Creates a new {@link CassandraEntityStoreFactory} instance.
     * 
     * @param host
     *            a Cassandra cluster host to connect to
     * @param port
     *            port number for Cassandra's native protocol
     * @param keyspace
     *            application keyspace
     * @param consistency
     *            default consistency level
     */
    @Inject
    public CassandraEntityStoreFactory(@Named("cassandraHost") String host, @Named("cassandraPort") int port, @Named("cassandraKeyspace") String keyspace, @Named("cassandraConsistency") ConsistencyLevel consistency) {

        checkNotNull(host, "Cassandra hostname");
        checkNotNull(port, "Cassandra port number");
        checkNotNull(keyspace, "Cassandra keyspace");
        checkNotNull(consistency, "Cassandra consistency level");

        m_consistency = consistency;

        Cluster cluster = Cluster.builder().withPort(port).addContactPoint(host).build();

        try {
            m_session = cluster.connect(keyspace);
        }
        catch (DriverException e) {
            throw new LucidityException(e);
        }

    }

    @Override
    public EntityStore createEntityStore() {
        return new CassandraEntityStore(m_session, m_consistency);
    }

}
