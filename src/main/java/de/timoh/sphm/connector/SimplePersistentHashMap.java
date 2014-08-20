package de.timoh.sphm.connector;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class SimplePersistentHashMap<K, V> extends HashMap<K, V> {

    private static final long serialVersionUID = -3452372538801457617L;

    private final ConnectorInformation connectorInformation;

    private final MapConnector<K, V> mapConnector;

    public SimplePersistentHashMap(ConnectorInformation connectorInformation, MapConnector<K, V> mapConnector) {
        super();
        this.connectorInformation = connectorInformation;
        this.mapConnector = mapConnector;
        initialize();
    }

    private void initialize() {
        try {
            this.mapConnector.initialize(this).load();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public V put(K key, V value) {
        try {
            mapConnector.put(key, value);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return super.put(key, value);
    }
    
    protected void putIntern(K key, V value) {
        super.put(key, value);
    }
    
    @Override
    public void putAll(Map<? extends K, ? extends V> map) {
        try {
            mapConnector.putAll(map);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        super.putAll(map);
    }

    @Override
    public V remove(Object key) {
        try {
            mapConnector.remove((K) key);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return super.remove(key);
    }

    /**
     *
     */
    public void reload() {
        super.clear();
        initialize();
    }

    /**
     *
     */
    public void forceSynchronization() {
        try {
            mapConnector.forceSynchronization();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public void forceClear() {
        try {
            mapConnector.forceClear();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    public void forceDelete() {
        try {
            mapConnector.forceDelete();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}
