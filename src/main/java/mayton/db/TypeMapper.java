package mayton.db;

import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;


public class TypeMapper {

    /**
     *
     * @param typeDescription
     * @return
     */
    @NotNull
    public String fromOrc(@NotNull TypeDescription typeDescription) {
        return "VARCHAR";
    }

    @NotNull
    public TypeDescription toOrc(@NotNull String databaseType, @Nullable Integer databaseLength, @Nullable Integer databasePrecision, boolean isNullable) {
        return TypeDescription.createString();
    }

}