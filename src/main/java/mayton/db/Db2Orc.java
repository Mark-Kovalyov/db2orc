package mayton.db;

import com.google.common.collect.ImmutableMap;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

import java.sql.*;
import java.util.Map;

public class Db2Orc {

    private static Map<String, TypeDescription> pgTypeDescMap = ImmutableMap.of(
            "varchar", TypeDescription.createString(),
            "float8",  TypeDescription.createDouble(),
            "float",   TypeDescription.createDouble(),
            "number",  TypeDescription.createInt()
    );

    @NotNull
    public static TypeDescription toOrc(@NotNull String typeName, boolean nullable) {
        if (pgTypeDescMap.containsKey(typeName)) {
            return pgTypeDescMap.get(typeName);
        } else {
            throw new IllegalArgumentException("Unable to map " + typeName + " to ORC datatypes");
        }
    }

    // Use case:
    // =========
    //
    // $ java db2orc.jar "jdbc:postgresql://localhost:5432/db" "user" "******" tables=EMP,DEBT
    //
    //
    //                jdbc:sqlserver://[serverName[\instanceName][:portNumber]][;property=value[;property=value]]
    //
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
            schema.addField(columnName, toOrc(typeName, nullAllowed == 1 ? true : false));
        }

        res.close();


        connection.close();
    }

}
