package de.timoh.sphm.connector.concurrent;

import de.timoh.sphm.connector.ConnectorInformation;
import de.timoh.sphm.connector.MapConnector;
import java.sql.Connection;
import java.sql.SQLException;
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

    public void addSQLJob(SQLJob<K, V> job) {
        synchronized (mapConnectorThread) {
            sqlJobQueue.add(job);
            mapConnectorThread.notify();
        }
    }

    @Override
    public void run() {
        synchronized (mapConnectorThread) {
            try {
                while (true) {
                    if (sqlJobQueue.isEmpty()) {
                        mapConnectorThread.wait();
                    }
                    SQLJob<K, V> sqlJob = sqlJobQueue.poll();
                    try (Connection con = getConnectorInfo().getConnection()) {
//                        synchronized (sqlJob) {
                        sqlJob.executeJob(con, getMap());
                        sqlJob.notifyAll();
//                        }
                    }
                }
            } catch (SQLException | InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

//    public boolean isJobDone(SQLJob<K, V> job) {
//        if (sqlJobQueue.contains(job)) {
//            synchronized (job) {
//                try {
//                    job.wait();
//                } catch (InterruptedException ex) {
//                    throw new RuntimeException(ex);
//                }
//                return true;
//            }
//        } else {
//            return true;
//        }
//    }
}
