package com.opennms.lucidity;

import static com.google.common.base.Preconditions.checkNotNull;

import javax.inject.Inject;
import javax.inject.Named;
import javax.inject.Singleton;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;


@Singleton
public class CassandraEntityStoreFactory implements EntityStoreFactory {

    private final Session m_session;
    private final ConsistencyLevel m_consistency;

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
