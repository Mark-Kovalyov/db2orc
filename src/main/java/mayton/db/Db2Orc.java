package mayton.db;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.cli.Options;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.Map;

public class Db2Orc extends GenericMainApplication {

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
        Options options = new Options();
        return options;
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
