package de.timoh.sphm.connector.concurrent;

import de.timoh.sphm.connector.ConnectorInformation;
import de.timoh.sphm.connector.MapConnector;
import static de.timoh.sphm.connector.MapConnector.BLOCK_INSERT_COUNT;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 *
 * @author <a href="mailto:timohanisch@gmail.com">Timo Hanisch</a>
 */
public class StrIntNonBlockingConnector extends ConcurrentMapConnector<String, Integer> {

    public StrIntNonBlockingConnector(ConnectorInformation connectorInfo) {
        super(connectorInfo);
    }

    /**
     * Not final documentation: Still blocking
     *
     * @param map
     * @return
     * @throws Exception
     */
    @Override
    public MapConnector<String, Integer> load(Map<String, Integer> map) throws Exception {
        getMap().clear();
        String stm = "SELECT * FROM " + getConnectorInfo().getTableName();
        String key;
        Integer value;
        try (Connection con = getConnectorInfo().getConnection(); PreparedStatement pStm = con.prepareStatement(stm); ResultSet resultSet = pStm.executeQuery();) {
            while (resultSet.next()) {
                key = resultSet.getString("key");
                value = resultSet.getInt("value");
                map.put(key, value);
            }
        }
        return this;
    }

    @Override
    public MapConnector<String, Integer> forceSynchronization() throws SQLException {
        super.addSQLJob(new SQLJob<String, Integer>() {
            @Override
            public void executeJob(Connection con, Map<String, Integer> map) throws SQLException {
                StrIntNonBlockingConnector.this.forceClear();
                int n = 0;
                StringBuilder values = new StringBuilder();
                String stm;
                for (String s : getMap().keySet()) {
                    if (n == BLOCK_INSERT_COUNT || n == getMap().size() - 1) {
                        stm = "INSERT INTO " + getConnectorInfo().getTableName() + "(key,value) VALUES " + values.toString() + ";";
                        try (PreparedStatement prepStm = con.prepareStatement(stm)) {
                            prepStm.execute();
                        }
                        values = new StringBuilder();
                        n = 0;
                    }
                    values = values.append("(").append("'").append(s).append("'").append(",").append(getMap().get(s)).append(")");
                    if (n + 1 != BLOCK_INSERT_COUNT && n + 1 != getMap().size() - 1) {
                        values = values.append(",");
                    }
                    n++;
                }
            }
        });
        return this;
    }

    /**
     * Still Blocking
     *
     * @param map
     * @return
     * @throws Exception
     */
    @Override
    public MapConnector<String, Integer> initialize(Map<String, Integer> map) throws Exception {
        setMap(map);
        try (Connection con = getConnectorInfo().getConnection()) {
            try (PreparedStatement prepStm = con.prepareStatement(getInsertIfExsists(getConnectorInfo().getTableName()))) {
                prepStm.execute();
            }
            if (!getConnectorInfo().tableExists()) {
                try (PreparedStatement prepStm = con.prepareStatement(getCreateTable(getConnectorInfo().getTableName()))) {
                    prepStm.execute();
                }
            }
        }
        return this;
    }

    @Override
    public MapConnector<String, Integer> put(final String key, final Integer value) throws Exception {
        super.addSQLJob(new SQLJob<String, Integer>() {
            @Override
            public void executeJob(Connection con, Map<String, Integer> map) throws SQLException {
                String stm;
                if (getMap().containsKey(key)) {
                    stm = "SELECT insertIfExistsStrInt('" + key + "', " + value + ");";
                } else {
                    stm = "INSERT INTO " + getConnectorInfo().getTableName() + "(key,value) VALUES ('" + key + "'," + value + ");";
                }
                try (PreparedStatement prepStm = con.prepareStatement(stm)) {
                    prepStm.execute();
                }
            }
        });
        return this;
    }

    @Override
    public MapConnector<String, Integer> putAll(final Map<? extends String, ? extends Integer> map) throws Exception {
        super.addSQLJob(new SQLJob<String, Integer>() {
            @Override
            public void executeJob(Connection con, Map<String, Integer> internalMap) throws SQLException {
                String stm = getCreateTableTmp(getConnectorInfo().getTableName());
                try (PreparedStatement prepStm = con.prepareStatement(stm)) {
                    prepStm.execute();
                }
                int n = 0;
                StringBuilder values = new StringBuilder();
                for (String s : getMap().keySet()) {
                    if (n == BLOCK_INSERT_COUNT || n == getMap().size() - 1) {
                        stm = "INSERT INTO " + getConnectorInfo().getTableName() + "tmp(key,value) VALUES " + values.toString() + ";";
                        try (PreparedStatement prepStm = con.prepareStatement(stm)) {
                            prepStm.execute();
                        }
                        values = new StringBuilder();
                        n = 0;
                    }
                    values = values.append("(").append("'").append(s).append("'").append(",").append(map.get(s)).append(")");
                    if (n + 1 != BLOCK_INSERT_COUNT && n + 1 != getMap().size() - 1) {
                        values = values.append(",");
                    }
                    n++;
                }
                stm = getMergeTables(getConnectorInfo().getTableName(), getConnectorInfo().getTableName() + "tmp");
                try (PreparedStatement prepStm = con.prepareStatement(stm)) {
                    prepStm.execute();
                }
                stm = "DROP TABLE " + getConnectorInfo().getTableName() + "tmp";
                try (PreparedStatement prepStm = con.prepareStatement(stm)) {
                    prepStm.execute();
                }
            }
        });
        return this;
    }

    @Override
    public MapConnector<String, Integer> remove(final String key) throws Exception {
        if (getMap().containsKey(key)) {
            super.addSQLJob(new SQLJob<String, Integer>() {
                @Override
                public void executeJob(Connection con, Map<String, Integer> map) throws SQLException {
                    String stm = "DELETE FROM " + getConnectorInfo().getTableName() + " WHERE key = '" + key + "'";
                    try (PreparedStatement prepStm = con.prepareStatement(stm)) {
                        prepStm.execute();
                    }
                }
            });
        }
        return this;
    }

    private static String getMergeTables(String table1, String table2) {
        return "INSERT INTO " + table1 + "\n"
                + "SELECT * FROM " + table2 + " \n"
                + "    WHERE NOT EXISTS(\n"
                + "            SELECT * FROM " + table1 + " \n"
                + "                 WHERE key=" + table2 + ".key \n"
                + "                       AND value=" + table2 + ".value \n"
                + "                     )";
    }

    private static String getCreateTable(String tableName) {
        return "CREATE TABLE " + tableName + "(\n"
                + "key varchar(255) PRIMARY KEY,\n"
                + "value integer\n"
                + ");";
    }

    private static String getCreateTableTmp(String tableName) {
        return "CREATE TABLE " + tableName + "tmp (\n"
                + "key varchar(255) PRIMARY KEY,\n"
                + "value integer\n"
                + ");";
    }

    private static String getInsertIfExsists(String tableName) {
        return "CREATE OR REPLACE FUNCTION insertIfExistsStrInt(k varchar(255),v integer) \n"
                + "        RETURNS integer AS $$\n"
                + "	BEGIN \n"
                + "		DELETE FROM " + tableName + " WHERE key = k;\n"
                + "		INSERT INTO " + tableName + "(key,value) VALUES (k,v);\n"
                + "		RETURN v;\n"
                + "	END;\n"
                + "	$$ LANGUAGE plpgsql;";
    }
}
