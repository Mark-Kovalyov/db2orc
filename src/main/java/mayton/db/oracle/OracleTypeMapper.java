package mayton.db.oracle;

import mayton.db.GenericTypeMapper;
import org.apache.orc.TypeDescription;
import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;

@ThreadSafe
public class OracleTypeMapper extends GenericTypeMapper {

    @Override
    public @NotNull String fromOrc(@NotNull TypeDescription typeDescription) {
        return "VARCHAR2(4000)";
    }

}
