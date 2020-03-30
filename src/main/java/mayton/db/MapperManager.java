package mayton.db;

import mayton.db.pg.PgTypeMapper;

public class MapperManager {

    ITypeMapper detect(String jdbcUrl) {
        // TODO: Stub
        return new PgTypeMapper();
    }

}
