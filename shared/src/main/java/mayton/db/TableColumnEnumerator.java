package mayton.db;

import org.jetbrains.annotations.NotNull;

import java.sql.Connection;

public interface TableColumnEnumerator {

    @NotNull Iterable<TableColumnEntity> entities(@NotNull Connection connection);

}
