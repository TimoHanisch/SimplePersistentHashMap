package de.timoh.sphm.connector.concurrent;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;

/**
 *
 * @author <a href="mailto:timohanisch@gmail.com">Timo Hanisch</a>
 * @param <K>
 * @param <V>
 */
public interface SQLJob<K, V> {
    
    void executeJob(Connection con, Map<K, V> map) throws SQLException;
}
