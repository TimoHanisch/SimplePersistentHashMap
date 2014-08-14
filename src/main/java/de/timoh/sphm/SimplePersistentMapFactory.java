package de.timoh.sphm;

import de.timoh.sphm.loader.ConnectorInformation;
import de.timoh.sphm.loader.StrIntConnector;
import java.sql.SQLException;
import java.util.Map;

/**
 *
 * @author Timo Hanisch (timohanisch@gmail.com)
 */
public class SimplePersistentMapFactory {

    public static Map<String, Integer> createStringIntegerSimplePersistentMap(String dbUrl, String dbUser, String dbPw, String tableName) throws SQLException {
        ConnectorInformation connectorInformation = new ConnectorInformation(dbUrl, dbUser, dbPw, tableName);
        return new SimplePersistentHashMap<>(connectorInformation, new StrIntConnector(connectorInformation));
    }
    
}
