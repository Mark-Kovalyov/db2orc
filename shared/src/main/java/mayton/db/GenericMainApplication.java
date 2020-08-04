package mayton.db;

import org.apache.commons.cli.Options;
import org.jetbrains.annotations.NotNull;

public abstract class GenericMainApplication {

    abstract @NotNull String createLogo();

    abstract @NotNull Options createOptions();

}
