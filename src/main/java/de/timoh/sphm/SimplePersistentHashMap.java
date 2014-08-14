package de.timoh.sphm;

import de.timoh.sphm.loader.ConnectorInformation;
import de.timoh.sphm.loader.MapConnector;
import java.sql.SQLException;
import java.util.HashMap;

public class SimplePersistentHashMap<K, V> extends HashMap<K, V> {

    private static final long serialVersionUID = -3452372538801457617L;

    private final ConnectorInformation connectorInformation;

    private final MapConnector<K, V> mapConnector;

    protected SimplePersistentHashMap(ConnectorInformation connectorInformation, MapConnector<K, V> mapConnector) throws SQLException {
        super();
        this.connectorInformation = connectorInformation;
        this.mapConnector = mapConnector;
    }

    private void load() {
        try {
            // Since the table does not exist we create one with the given name
            if (!connectorInformation.tableExists()) {

            }
            mapConnector.load();
        } catch (SQLException e) {
            //LOG.error(e);
        }
    }

    @Override
    public V put(K key, V value) {
        try {
            mapConnector.put(key, value);
        } catch (SQLException ex) {

        }
        return super.put(key, value);
    }

    @Override
    public V remove(Object key) {
        try {
            mapConnector.remove((K) key);
        } catch (SQLException ex) {

        }
        return super.remove(key);
    }

    /**
     *
     */
    public void reload() {
        super.clear();
        load();
    }

    /**
     *
     */
    public void forceSynchronization() {
        try {
            mapConnector.forceSynchronization();
        } catch (SQLException ex) {

        }
    }

    public void close() {
        try {
            connectorInformation.close();
        } catch (SQLException ex) {

        }
    }
}
