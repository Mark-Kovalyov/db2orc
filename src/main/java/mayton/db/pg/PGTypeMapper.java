package mayton.db.pg;

import mayton.db.TypeMapper;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class PGTypeMapper extends TypeMapper {

    @Override
    public String fromOrc(TypeDescription typeDescription) {
        return "TEXT";
    }

    @Override
    public @NotNull TypeDescription toOrc(@NotNull String databaseType, @Nullable Integer databaseLength, @Nullable Integer databasePrecision, boolean isNullable) {
        return super.toOrc(databaseType, databaseLength, databasePrecision, isNullable);
    }
}
