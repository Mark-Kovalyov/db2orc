package mayton.db;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;

public class OrcUtils {

    private OrcUtils(){}

    public static Writer createWriter(@NotNull FileSystem fs, @NotNull String path, @NotNull TypeDescription schema) throws IOException {

        Path testFilePath = new Path(path);
        return OrcFile.createWriter(testFilePath, OrcFile.writerOptions(fs.getConf())
                .setSchema(schema)
                .compress(CompressionKind.NONE)
                .stripeSize(128 * 1024 * 1024)
                .bufferSize(256 * 1024)
                .rowIndexStride(10000)
                .version(OrcFile.Version.V_0_12));
    }

}
