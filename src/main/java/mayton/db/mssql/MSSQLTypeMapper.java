package mayton.db.mssql;

import mayton.db.TypeMapper;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

public class MSSQLTypeMapper extends TypeMapper {

    @Override
    public @NotNull String fromOrc(@NotNull TypeDescription typeDescription) {
        return "VARCHAR";
    }

}
