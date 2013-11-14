package com.opennms.lucidity;


public interface EntityStoreFactory {

    /**
     * Creates and returns a new {@link EntityStore}.
     * 
     * @return an entity store.
     */
    EntityStore createEntityStore();

}
