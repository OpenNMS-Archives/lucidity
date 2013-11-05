package com.opennms.cassandra.mapper;


public class StorageException extends Exception {

    private static final long serialVersionUID = 1L;

    public StorageException(String msg) {
        super(msg);
    }

}
