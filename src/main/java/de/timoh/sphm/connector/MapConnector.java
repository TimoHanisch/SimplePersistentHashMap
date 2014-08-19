package de.timoh.sphm.connector;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;

/**
 *
 * @author Timo Hanisch (timohanisch@gmail.com)
 * @param <K>
 * @param <V>
 */
public abstract class MapConnector<K, V> {
    
    public static final int BLOCK_INSERT_COUNT = 500;
    
    private final ConnectorInformation connectorInfo;

    private Map<K, V> map;

    public MapConnector(ConnectorInformation connectorInfo) {
        this.connectorInfo = connectorInfo;
    }

    public abstract MapConnector<K, V> initialize(Map<K, V> map) throws Exception;

    public abstract MapConnector<K, V> load() throws Exception;

    public abstract MapConnector<K, V> forceSynchronization() throws Exception;

    public abstract MapConnector<K, V> put(K key, V value) throws Exception;

    public abstract MapConnector<K, V> putAll(Map<? extends K, ? extends V> map) throws Exception;
    
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
