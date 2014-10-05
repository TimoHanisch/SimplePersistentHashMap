package de.timoh.sphm.connector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 * The abstract class, which should be implemented by all hash map types. That
 * means if there is a new datatype wanted a new implementation has to be done.
 *
 * Each abstract method returns the MapConnector itself, to allow concats of methods.
 * @author <a href="mailto:timohanisch@gmail.com">Timo Hanisch</a>
 * @param <K>
 * @param <V>
 */
public abstract class MapConnector<K, V> {

    // The number of blocks that can be inserted at the same time via synchronization
    public static final int BLOCK_INSERT_COUNT = 500;

    private final ConnectorInformation connectorInfo;

    private Map<K, V> map;

    public MapConnector(ConnectorInformation connectorInfo) {
        this.connectorInfo = connectorInfo;
    }

    /**
     * Initializing the map by creating the table and functions needed.
     * @param map
     * @return
     * @throws Exception 
     */
    public abstract MapConnector<K, V> initialize(Map<K, V> map) throws Exception;

    /**
     * Loads all elements from the database into the map.
     * 
     * @param map
     * @return
     * @throws Exception 
     */
    public abstract MapConnector<K, V> load(Map<K, V> map) throws Exception;

    /**
     * Forces the connected map to synchronize its values with the database.
     * @return
     * @throws Exception 
     */
    public abstract MapConnector<K, V> forceSynchronization() throws Exception;

    /**
     * Puts a key value pair into the table.
     * @param key
     * @param value
     * @return
     * @throws Exception 
     */
    public abstract MapConnector<K, V> put(K key, V value) throws Exception;

    /**
     * Puts all key value pairs from a map into the table. Allows faster insertions.
     * @param map
     * @return
     * @throws Exception 
     */
    public abstract MapConnector<K, V> putAll(Map<? extends K, ? extends V> map) throws Exception;

    /**
     * Removes a key-value pair from the table.
     * @param key
     * @return
     * @throws Exception 
     */
    public abstract MapConnector<K, V> remove(K key) throws Exception;

    public void forceClear() throws SQLException {
        String stm = "TRUNCATE " + connectorInfo.getTableName() + ";";
        try (Connection con = connectorInfo.getConnection(); PreparedStatement prepStm = con.prepareStatement(stm)) {
            prepStm.execute();
        }
    }

    public void forceDelete() throws SQLException {
        String stm = "DROP TABLE " + connectorInfo.getTableName() + ";";
        try (Connection con = connectorInfo.getConnection(); PreparedStatement prepStm = con.prepareStatement(stm)) {
            prepStm.execute();
        }
    }

    public ConnectorInformation getConnectorInfo() {
        return connectorInfo;
    }

    protected void setMap(Map<K, V> map) {
        this.map = map;
    }

    public Map<K, V> getMap() {
        return map;
    }
}
