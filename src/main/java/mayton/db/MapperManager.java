package mayton.db;

import mayton.db.maria.MariaDbTypeMapper;
import mayton.db.mssql.MsSqlTypeMapper;
import mayton.db.mysql.MySqlMapper;
import mayton.db.oracle.OracleTypeMapper;
import mayton.db.pg.PgTypeMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jetbrains.annotations.NotNull;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapperManager {

    public static Logger logger = LogManager.getLogger(MapperManager.class);

    public static MapperManager instance = new MapperManager();

    private MapperManager() {}

    @NotNull
    ITypeMapper detect(@NotNull String jdbcUrl) {
        Pattern jdbcPattern = Pattern.compile("^(?<prefix>jdbc:[a-z]+:).+");
        Matcher matcher = jdbcPattern.matcher(jdbcUrl);
        if (matcher.matches()) {
            String jdbcUrlPrefix = matcher.group("prefix");
            logger.info("jdbcUrlPrefix = {}", jdbcUrlPrefix);
            switch (jdbcUrlPrefix) {
                case "jdbc:postgresql:":
                    return new PgTypeMapper();
                case "jdbc:oracle:":
                    return new OracleTypeMapper();
                case "jdbc:sqlserver:":
                    return new MsSqlTypeMapper();
                case "jdbc:mysql:":
                    return new MySqlMapper();
                case "jdbc:mariadb:":
                    return new MariaDbTypeMapper();
                default:
                    logger.error("Warning! Unable to detect MapperManager from url = {}. Trying to use default.", jdbcUrl);
                    return new GenericTypeMapper();
            }
        } else {
            throw new IllegalArgumentException("jdbcUrl doesnt match to pattern " + jdbcPattern.pattern());
        }
    }

}
