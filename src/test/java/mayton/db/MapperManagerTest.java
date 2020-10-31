package mayton.db;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class MapperManagerTest {

    @Test
    public void test() {
        ITypeMapper typeMapper = MapperManager.instance.detect("jdbc:postgresql://127.0.0.1:5432/postgres");
        assertNotNull(typeMapper);
        assertEquals("mayton.db.pg.PgTypeMapper", typeMapper.getClass().getName());
    }

}
