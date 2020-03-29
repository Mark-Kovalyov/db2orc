package mayton.db;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;

import java.io.IOException;
import java.util.Properties;

public class OrcUtils {

    private OrcUtils(){}

    public static Writer createWriter(@NotNull FileSystem fileSystem, @NotNull String destPath, @NotNull TypeDescription schema, @NotNull Properties properties) throws IOException {
        Path destHadoopPath = new Path(destPath);
        OrcFile.WriterOptions opts = OrcFile.writerOptions(fileSystem.getConf()).setSchema(schema);
        opts = opts.compress(CompressionKind.valueOf("NONE"));
        opts = opts.rowIndexStride(0);
        opts = opts.bloomFilterColumns("id,raion,adr,tel,datar,pasps,paspn,wd,fam,im,otch");
        opts = opts.bloomFilterFpp(0.97);

        // OK
        /*if (properties.contains("orc.compression")) {
            opts = opts.compress(CompressionKind.valueOf(properties.getProperty("orc.compression")));
        }
        if (properties.contains("orc.bloomcolumns")) {
            opts = opts.bloomFilterColumns(properties.getProperty("orc.bloomcolumns"));
        }
        if (properties.contains("orc.rowindexstride")) {
            opts = opts.rowIndexStride(Integer.parseInt(properties.getProperty("orc.rowindexstride")));
        }
        // TODO: Not tested yet
        if (properties.contains("orc.stripeSize")) opts.stripeSize(Long.parseLong(properties.getProperty("orc.stripeSize")));
        if (properties.contains("orc.bufferSize")) opts.bufferSize(Integer.parseInt(properties.getProperty("orc.bufferSize")));

        if (properties.contains("orc.bloomFilterFpp")) opts.bloomFilterFpp(Double.parseDouble(properties.getProperty("orc.bloomFilterFpp")));*/
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
