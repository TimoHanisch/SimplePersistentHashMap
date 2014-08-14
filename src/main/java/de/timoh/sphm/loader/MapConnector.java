package de.timoh.sphm.loader;

import java.sql.SQLException;
import java.util.Map;

/**
 *
 * @author Timo Hanisch (timohanisch@gmail.com)
 * @param <K>
 * @param <V>
 */
public abstract class MapConnector<K, V> {

    private final ConnectorInformation connectorInfo;

    private Map<K, V> map;

    public MapConnector(ConnectorInformation connectorInfo) {
        this.connectorInfo = connectorInfo;
    }

    public abstract void initialize(Map<K,V> map) throws SQLException;

    public abstract void load() throws SQLException;

    public abstract void forceSynchronization() throws SQLException;

    public abstract void put(K key, V value) throws SQLException;

    public abstract V remove(K key) throws SQLException;

    public ConnectorInformation getConnectorInfo() {
        return connectorInfo;
    }

    protected void setMap(Map<K,V> map) {
        this.map = map;
    }
    
    public Map<K, V> getMap() {
        return map;
    }
}
