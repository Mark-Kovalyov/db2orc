package mayton.db.pg;

import mayton.db.TableColumnEntity;
import mayton.db.TableColumnEnumerator;
import org.jetbrains.annotations.NotNull;

import java.sql.Connection;

public class PGTableColumnEnumerator implements TableColumnEnumerator {

    @Override
    public @NotNull Iterable<TableColumnEntity> entities(@NotNull Connection connection) {
        return null;
    }
}
