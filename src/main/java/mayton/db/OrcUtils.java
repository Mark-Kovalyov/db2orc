package mayton.db;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.IOException;
import java.util.Properties;

public class OrcUtils {

    public static Logger logger = LogManager.getLogger(OrcUtils.class);

    private OrcUtils(){}

    public static Writer createWriter(@NotNull FileSystem fileSystem, @NotNull String destPath, @NotNull TypeDescription schema, @NotNull Properties properties) throws IOException {
        Path destHadoopPath = new Path(destPath);
        OrcFile.WriterOptions opts = OrcFile.writerOptions(fileSystem.getConf()).setSchema(schema);

        if (properties.containsKey("orc.compression"))
            opts = opts.compress(CompressionKind.valueOf(properties.getProperty("orc.compression")));

        if (properties.containsKey("orc.rowindexstride"))
            opts = opts.rowIndexStride(Integer.parseInt(properties.getProperty("orc.rowindexstride")));

        if (properties.containsKey("orc.bloomcolumns"))
            opts = opts.bloomFilterColumns(properties.getProperty("orc.bloomcolumns"));

        if (properties.containsKey("orc.bloomfilterfpp"))
            opts = opts.bloomFilterFpp(Double.parseDouble(properties.getProperty("orc.bloomfilterfpp")));

        if (properties.contains("orc.stripesize"))
            opts = opts.stripeSize(Long.parseLong(properties.getProperty("orc.stripesize")));

        opts = opts.enforceBufferSize();

        return OrcFile.createWriter(destHadoopPath, opts);
    }

    @TestOnly
    public static Writer createWriter(@NotNull FileSystem fileSystem, @NotNull String destPath, @NotNull TypeDescription schema) throws IOException {
        Path destHadoopPath = new Path(destPath);
        return OrcFile.createWriter(destHadoopPath, OrcFile.writerOptions(fileSystem.getConf())
                .setSchema(schema)
                .compress(CompressionKind.NONE)
                .stripeSize(128L * 1024 * 1024)
                .bufferSize(256 * 1024)
                .rowIndexStride(10000)
                .version(OrcFile.Version.V_0_12));
    }

}
