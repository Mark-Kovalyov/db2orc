package mayton.db;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Properties;

public class OrcUtils {

    private OrcUtils(){}

    public static Writer createWriter(@NotNull FileSystem fileSystem, @NotNull String destPath, @NotNull TypeDescription schema, @NotNull Properties properties) throws IOException {
        Path destHadoopPath = new Path(destPath);
        OrcFile.WriterOptions opts = OrcFile.writerOptions(fileSystem.getConf()).setSchema(schema);
        // TODO: Map
        if (properties.contains("orc.compress")) opts.compress(CompressionKind.valueOf(properties.getProperty("orc.compress")));
        if (properties.contains("orc.stripeSize")) opts.stripeSize(Long.parseLong(properties.getProperty("orc.stripeSize")));
        if (properties.contains("orc.bufferSize")) opts.bufferSize(Integer.parseInt(properties.getProperty("orc.bufferSize")));
        if (properties.contains("orc.rowIndexStride")) opts.rowIndexStride(Integer.parseInt(properties.getProperty("orc.rowIndexStride")));
        if (properties.contains("orc.bloomFilterColumns")) opts.bloomFilterColumns(properties.getProperty("orc.bloomFilterColumns"));
        if (properties.contains("orc.bloomFilterFpp")) opts.bloomFilterFpp(Double.parseDouble(properties.getProperty("orc.bloomFilterFpp")));
        return OrcFile.createWriter(destHadoopPath, opts);
    }

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
