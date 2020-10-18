package mayton.db;

import org.jetbrains.annotations.NotNull;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

public class SQLUtils {

    private SQLUtils() {}

    @NotNull
    public static ResultSet getColumns(@NotNull DatabaseMetaData metadata, @NotNull String tableName) throws SQLException {
        return metadata.getColumns(null, null, tableName, null);
    }
}
