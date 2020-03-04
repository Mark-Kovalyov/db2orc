package mayton.db;

import com.google.common.collect.ImmutableMap;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.Map;

public class Db2Orc {


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
