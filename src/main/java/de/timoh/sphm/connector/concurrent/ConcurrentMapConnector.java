package de.timoh.sphm.connector.concurrent;

import de.timoh.sphm.connector.ConnectorInformation;
import de.timoh.sphm.connector.MapConnector;
import java.sql.Connection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 *
 * @author Timo Hanisch (timohanisch@gmail.com)
 * @param <K>
 * @param <V>
 */
public abstract class ConcurrentMapConnector<K, V> extends MapConnector<K, V> implements Runnable {

    private final Thread mapConnectorThread;

    private final Queue<SQLJob<K, V>> sqlJobQueue = new ConcurrentLinkedQueue<>();

    public ConcurrentMapConnector(ConnectorInformation connectorInfo) {
        super(connectorInfo);
        mapConnectorThread = new Thread(this);
        mapConnectorThread.start();
    }

    @Override
    public void run() {
        try {
            while (true) {
                if (sqlJobQueue.isEmpty()) {
                    mapConnectorThread.wait();
                }
                SQLJob<K, V> sqlJob = sqlJobQueue.poll();
                try (Connection con = getConnectorInfo().getConnection()) {
                    sqlJob.executeJob(con, getMap());
                }
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
