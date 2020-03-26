package mayton.db;

import mayton.db.pg.PGTypeMapper;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Properties;

public class Db2Orc extends GenericMainApplication {

    public static Logger logger = LogManager.getLogger(Db2Orc.class);

    private static final boolean DEVMODE = false;

    static String logo =
          "\n     888 888       .d8888b.                           \n" +
            "     888 888      d88P  Y88b                          \n" +
            "     888 888             888                          \n" +
            " .d88888 88888b.       .d88P  .d88b.  888d888 .d8888b \n" +
            "d88\" 888 888 \"88b  .od888P\"  d88\"\"88b 888P\"  d88P\"    \n" +
            "888  888 888  888 d88P\"      888  888 888    888      \n" +
            "Y88b 888 888 d88P 888\"       Y88..88P 888    Y88b.    \n" +
            " \"Y88888 88888P\"  888888888   \"Y88P\"  888     \"Y8888P\n\n";

    @Override
    Options createOptions() {
        return new Options()
                .addRequiredOption("u", "url",       true, "JDBC url. (ex:jdbc:oracle:thin@localhost:1521/XE")
                .addRequiredOption("l", "login",     true, "JDBC login")
                .addRequiredOption("p", "password",  true, "JDBC password")
                .addRequiredOption("o", "orcfile",   true, "Orc file. (ex:big-data.orc)")
                .addOption("s", "selectexpr",true, "SELECT-expression")
                .addOption("t", "tablename", true, "Table or View name (excludes select expression)")
                .addOption("co", "orc.compression",  true, "Orc file compression := { NONE, ZLIB, SNAPPY, LZO, LZ4, ZSTD }")
                .addOption("bf", "orc.bloomColumns", true, "Orc file bloom filter columns (comma-separated)");
    }

    private static void appendRow(VectorizedRowBatch batch, int row, int v) {
        // struct<book_ref:string,
        //           book_date:timestamp,
        //           total_amount:int>
        // TODO: Map
        int i = 0;
        ((BytesColumnVector)     batch.cols[i++]).setVal(row, ("book_ref" + i).getBytes(StandardCharsets.UTF_8));
        ((TimestampColumnVector) batch.cols[i++]).set(row, new Timestamp(System.currentTimeMillis()));
        ((LongColumnVector)      batch.cols[i++]).vector[row] = v;
    }

    public void processWithWriter(@NotNull Writer writer, @NotNull TypeDescription schema, @NotNull Connection connection, @NotNull String query) throws IOException {
        // TODO: Hardcode
        int batchSize = 50000;
        VectorizedRowBatch batch = schema.createRowBatch(batchSize);
        // TODO: Hardcode
        int numRows = 200;
        int tail = 0;
        // TODO: Fetch from SQL but fake
        for (int b = 0; b < numRows; tail = b++ % batchSize) {
            appendRow(batch, batch.size++, b);
            if (tail == 0) {
                writer.addRowBatch(batch);
                batch.reset();
            }
        }
        if (tail != 0) {
            writer.addRowBatch(batch);
            batch.reset();
        }
    }

    public void process(@NotNull Properties properties) throws SQLException, ClassNotFoundException, IOException {
        logger.info("[1] Start process");

        Connection connection = DriverManager.getConnection(
                properties.getProperty("url"),
                properties.getProperty("login"),
                properties.getProperty("password"));

        logger.info("[2] Read metadata from DB");

        DatabaseMetaData metadata = connection.getMetaData();

        String tableName = properties.getProperty("tablename");

        ResultSet res = SQLUtils.getColumns(metadata, tableName);

        TypeDescription schema = TypeDescription.createStruct();

        logger.info("[3] Process type mapper");

        TypeMapper typeMapper = new PGTypeMapper();

        while(res.next()) {
            String columnName = res.getString("COLUMN_NAME");
            int dataType      = res.getInt("DATA_TYPE");
            String typeName   = res.getString("TYPE_NAME");
            int nullAllowed   = res.getInt("NULLABLE");
            logger.info("{} , dataType = {}, typeName = {}, nullAllowred = {}", columnName, dataType, typeName, nullAllowed);
            // TODO: Add length, precission, nullable
            TypeDescription typeDescription = typeMapper.toOrc(typeName, 30, 0, true);
            logger.info("typeDescription = {}", typeDescription);
            schema.addField(columnName, typeDescription);
        }

        res.close();

        String orcFilePath = properties.getProperty("orcfile");
        logger.info("[4] Export ORC file = {}", orcFilePath);
        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        String userDir = System.getProperty("user.dir");
        logger.info("[4.3] User dir = {}", userDir);
        org.apache.hadoop.fs.Path currentDirPath = new org.apache.hadoop.fs.Path(userDir);
        logger.info("[5] currentDirPath = {}", currentDirPath.toString());
        org.apache.hadoop.fs.FileSystem currentDirPathFileSystem = currentDirPath.getFileSystem(conf);
        logger.info("[6] fs.canonicalServName = {}", currentDirPathFileSystem.getCanonicalServiceName());
        currentDirPathFileSystem.delete(new Path(orcFilePath), false);
        logger.info("[6.1] create Orc-Writer with schema");
        Writer writer = OrcUtils.createWriter(currentDirPathFileSystem, orcFilePath, schema, properties);
        processWithWriter(writer, schema, connection, tableName);
        writer.close();
        logger.info("[6.2] Orc-Writer closed");
        connection.close();
        logger.info("[7] Finish!");
    }

    public void process(String[] args) throws SQLException, ParseException, IOException, ClassNotFoundException {

        Properties properties = new Properties();
        if (DEVMODE) {
            properties.load(new FileInputStream("sensitive.properties"));
        } else {
            CommandLineParser parser = new DefaultParser();
            Options options = createOptions();
            if (args.length == 0) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(logo, createOptions());
                return;
            } else {
                CommandLine line = parser.parse(options, args);
                properties.put("url", line.getOptionValue("u"));
                properties.put("login", line.getOptionValue("l"));
                properties.put("password", line.getOptionValue("p"));
                if (line.hasOption("t")) properties.put("tablename", line.getOptionValue("t"));
                if (line.hasOption("s")) properties.put("selectexpr", line.getOptionValue("s"));
                properties.put("orcfile", line.getOptionValue("o"));
            }
        }
        process(properties);
    }

    public static void main(String[] args) throws SQLException, ParseException, IOException, ClassNotFoundException {
        System.setProperty("log4j1.compatibility", "true");
        System.setProperty("log4j.configuration", "log4j.properties");
        new Db2Orc().process(args);
    }


}
