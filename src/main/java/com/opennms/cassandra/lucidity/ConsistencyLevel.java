package com.opennms.cassandra.lucidity;


public enum ConsistencyLevel {

    ANY(0), ONE(1), TWO(2), THREE(3), QUORUM(4), ALL(5), LOCAL_QUORUM(6), EACH_QUORUM(7), SERIAL(8), LOCAL_SERIAL(9);

    private final int m_code;

    private ConsistencyLevel(int code) {
        m_code = code;
    }

    int getDriverCode() {
        return m_code;
    }

}
