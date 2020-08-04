package mayton.db;

import mayton.db.pg.PgTypeMapper;
import org.jetbrains.annotations.NotNull;

public class MapperManager {

    public static MapperManager instance = new MapperManager();

    private MapperManager() {}

    ITypeMapper detect(@NotNull String jdbcUrl) {
        // TODO: Stub
        if (jdbcUrl.startsWith("jdbc:postgresql:")) {
            return new PgTypeMapper();
        } else {
            throw new IllegalArgumentException("Unable to detect TypeMapper for jdbc url = " + jdbcUrl);
        }
    }

}
