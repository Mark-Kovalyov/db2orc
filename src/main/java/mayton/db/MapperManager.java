package mayton.db;

import mayton.db.mssql.MsSqlTypeMapper;
import mayton.db.mysql.MySqlMapper;
import mayton.db.oracle.OracleTypeMapper;
import mayton.db.pg.PgTypeMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

public class MapperManager {

    public static Logger logger = LogManager.getLogger(MapperManager.class);

    public static MapperManager instance = new MapperManager();

    private MapperManager() {}

    @NotNull
    ITypeMapper detect(@NotNull String jdbcUrl) {
        if (jdbcUrl.startsWith("jdbc:postgresql:")) {
            return new PgTypeMapper();
        } else if (jdbcUrl.startsWith("jdbc:oracle:")) {
            return new OracleTypeMapper();
        } else if (jdbcUrl.startsWith("jdbc:sqlserver:")) {
            return new MsSqlTypeMapper();
        } else if (jdbcUrl.startsWith("jdbc:mysql:")) {
            return new MySqlMapper();
        } else {
            logger.warn("Warning! Unable to detect MapperManager from url = {}. Trying to use default.", jdbcUrl);
            return new GenericTypeMapper();
        }
    }

}
