package mayton.db;

import javax.annotation.concurrent.Immutable;

@Immutable
public final class TableColumnEntity {

    public final String columnName;

    public TableColumnEntity(String columnName) {
        this.columnName = columnName;
    }
}
