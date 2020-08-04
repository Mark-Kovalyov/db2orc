package mayton.db.mssql;

import mayton.db.GenericTypeMapper;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

public class MsSqlTypeMapper extends GenericTypeMapper {

    @Override
    public @NotNull String fromOrc(@NotNull TypeDescription typeDescription) {
        return "VARCHAR";
    }

}
