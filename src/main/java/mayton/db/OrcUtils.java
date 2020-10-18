package mayton.db;

import org.apache.commons.cli.CommandLine;
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

public class OrcUtils {

    public static Logger logger = LogManager.getLogger(OrcUtils.class);

    private OrcUtils(){}

    public static Writer createWriter(@NotNull FileSystem fileSystem, @NotNull String destPath, @NotNull TypeDescription schema, @NotNull CommandLine line) throws IOException {
        Path destHadoopPath = new Path(destPath);
        OrcFile.WriterOptions opts = OrcFile.writerOptions(fileSystem.getConf()).setSchema(schema);

        if (line.hasOption("orc.compression"))
            opts = opts.compress(CompressionKind.valueOf(line.getOptionValue("orc.compression")));

        if (line.hasOption("orc.rowindexstride"))
            opts = opts.rowIndexStride(Integer.parseInt(line.getOptionValue("orc.rowindexstride")));

        if (line.hasOption("orc.bloomcolumns")) {
            String bloomColumns = line.getOptionValue("orc.bloomcolumns");
            logger.trace("Detected bloomColumns = {}", bloomColumns);
            opts = opts.bloomFilterColumns(bloomColumns);
        }

        if (line.hasOption("orc.bloomfilterfpp"))
            opts = opts.bloomFilterFpp(Double.parseDouble(line.getOptionValue("orc.bloomfilterfpp")));

        if (line.hasOption("orc.stripesize"))
            opts = opts.stripeSize(Long.parseLong(line.getOptionValue("orc.stripesize")));

        if (line.hasOption("orc.buffersize"))
            opts = opts.bufferSize(Integer.parseInt(line.getOptionValue("orc.buffersize")));

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
