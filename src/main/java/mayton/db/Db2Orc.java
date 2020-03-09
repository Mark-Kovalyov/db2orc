package mayton.db;

import org.apache.commons.cli.Options;
import org.apache.orc.TypeDescription;

import java.sql.*;

public class Db2Orc extends GenericMainApplication {

    private static final boolean DEVMODE = false;

    static String logo =
            "============================================================================================" +
            "     888 888       .d8888b.                           \n" +
            "     888 888      d88P  Y88b                          \n" +
            "     888 888             888                          \n" +
            " .d88888 88888b.       .d88P  .d88b.  888d888 .d8888b \n" +
            "d88\" 888 888 \"88b  .od888P\"  d88\"\"88b 888P\"  d88P\"    \n" +
            "888  888 888  888 d88P\"      888  888 888    888      \n" +
            "Y88b 888 888 d88P 888\"       Y88..88P 888    Y88b.    \n" +
            " \"Y88888 88888P\"  888888888   \"Y88P\"  888     \"Y8888P ";

    @Override
    Options createOptions() {
        return new Options()
                .addOption("u", "url",       true, "JDBC url. (ex:jdbc:oracle:thin@localhost:1521/XE")
                .addOption("l", "login",     true, "JDBC login")
                .addOption("p", "password",  true, "JDBC password")
                .addOption("o", "orcfile",   true, "Orc file. (ex:big-data.orc)")
                .addOption("s", "selectexpr",true, "SELECT-expression")
                .addOption("t", "tablename", true, "Table or View name (excludes select expression)");
    }

    public static void main(String[] args) throws SQLException {

        String url      = args[0];
        String user     = args[1];
        String password = args[2];
        String query = "SELECT * FROM geoipcity";
        Connection connection = DriverManager.getConnection(url, user, password);

        DatabaseMetaData metadata = connection.getMetaData();

        ResultSet res = metadata.getColumns(null, null, "geoipcity", null);

        TypeDescription schema = TypeDescription.createStruct();

        while(res.next()) {
            String columnName = res.getString("COLUMN_NAME");
            int dataType      = res.getInt("DATA_TYPE");
            String typeName   = res.getString("TYPE_NAME");
            int nullAllowed   = res.getInt("NULLABLE");
            System.out.printf("%s , dataType = %d, typeName = %s, nullAllowred = %d\n", columnName, dataType, typeName, nullAllowed);
        }

        res.close();


        connection.close();
    }


}
