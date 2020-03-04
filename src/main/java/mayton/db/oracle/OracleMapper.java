package mayton.db.oracle;

import mayton.db.TypeMapper;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

public class OracleMapper extends TypeMapper {
    @Override
    public @NotNull String fromOrc(@NotNull TypeDescription typeDescription) {
        return "VARCHAR2(4000)";
    }
}
