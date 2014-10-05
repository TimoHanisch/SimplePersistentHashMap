package de.timoh.sphm;

import de.timoh.sphm.connector.ConnectorInformation;
import de.timoh.sphm.connector.MapConnector;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * The SimplePersistentHashMap overrides the HashMap and offers some persistent
 * methods.
 * 
 * @author <a href="mailto:timohanisch@gmail.com">Timo Hanisch</a>
 * @param <K>
 * @param <V> 
 */
public class SimplePersistentHashMap<K, V> extends HashMap<K, V> {

    private static final long serialVersionUID = -3452372538801457617L;

    private final ConnectorInformation connectorInformation;

    private final MapConnector<K, V> mapConnector;

    protected SimplePersistentHashMap(ConnectorInformation connectorInformation, MapConnector<K, V> mapConnector) {
        super();
        this.connectorInformation = connectorInformation;
        this.mapConnector = mapConnector;
        initialize();
    }

    private void initialize() {
        try {
            Map<K, V> map = new HashMap<>();
            this.mapConnector.initialize(this).load(map);
            super.putAll(map);
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
     * Reloads all elements from the database into the map.
     */
    public void reload() {
        super.clear();
        initialize();
    }

    /**
     * Forces the map to synchronize with the database, meaning all elements get
     * rewritten to the database.
     */
    public void forceSynchronization() {
        try {
            mapConnector.forceSynchronization();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     * Clears the map and the table in the database.
     */
    public void forceClear() {
        try {
            mapConnector.forceClear();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    /**
     * Delets the table in the database.
     */
    public void forceDelete() {
        try {
            mapConnector.forceDelete();
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
    }
}
