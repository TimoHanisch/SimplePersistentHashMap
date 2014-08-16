package de.timoh.sphm.loader;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 *
 * @author Timo Hanisch (timohanisch@gmail.com)
 * @param <K>
 * @param <V>
 */
public abstract class MapConnector<K, V> {
    
    public static final int BLOCK_INSERT_COUNT = 500;
    
    public static final int WORKER_THREADS = 10;
    
    private static ExecutorService executor = Executors.newFixedThreadPool(WORKER_THREADS);
    
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

    public void forceClear() throws Exception {
        String stm = "TRUNCATE " + connectorInfo.getTableName() + ";";
        try (Connection con = connectorInfo.getConnection(); PreparedStatement prepStm = con.prepareStatement(stm)) {
            prepStm.execute();
        }
    }

    public void forceDelete() throws Exception {
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
    
    public void executeRunnable(Runnable r) {
        executor.execute(r);
    }
}
