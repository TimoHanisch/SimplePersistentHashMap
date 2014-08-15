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

    public abstract MapConnector<K, V> initialize(Map<K,V> map) throws Exception;

    public abstract MapConnector<K, V> load() throws Exception;

    public abstract MapConnector<K, V> forceSynchronization() throws SQLException;

    public abstract MapConnector<K, V> put(K key, V value) throws SQLException;

    public abstract MapConnector<K, V> remove(K key) throws SQLException;
    
    public void forceClear() throws SQLException{
        String stm = "TRUNCATE "+connectorInfo.getTableName()+";";
        connectorInfo.getConnection().prepareStatement(stm).execute();
    }

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
