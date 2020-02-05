package mayton.db;

import org.apache.orc.TypeDescription;

public abstract class TypeMapper {

  public abstract String fromOrc(TypeDescription typeDescription);

}