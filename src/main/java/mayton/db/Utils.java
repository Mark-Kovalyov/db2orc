package mayton.db;

import org.jetbrains.annotations.NotNull;

import java.io.PrintStream;

public class Utils {

    private Utils() {}

    public static void println(@NotNull String arg) {
        System.out.println(arg);
    }

    public static PrintStream printf(@NotNull String format, Object ...args) {
        return System.out.printf(format, args);
    }

}
