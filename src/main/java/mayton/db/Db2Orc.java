package mayton.db;

import mayton.db.pg.PgTypeMapper;
import mayton.db.pg.PgTypes;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jetbrains.annotations.NotNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.Optional;
import java.util.Properties;

import static java.lang.Integer.parseInt;
import static java.nio.charset.StandardCharsets.UTF_8;

@SuppressWarnings("java:S2629")
public class Db2Orc extends GenericMainApplication {

    public static Logger logger = LogManager.getLogger(Db2Orc.class);

    @NotNull
    @Override
    String createLogo() {
        return "\n     888 888       .d8888b.                           \n" +
                "     888 888      d88P  Y88b                          \n" +
                "     888 888             888                          \n" +
                " .d88888 88888b.       .d88P  .d88b.  888d888 .d8888b \n" +
                "d88\" 888 888 \"88b  .od888P\"  d88\"\"88b 888P\"  d88P\"    \n" +
                "888  888 888  888 d88P\"      888  888 888    888      \n" +
                "Y88b 888 888 d88P 888\"       Y88..88P 888    Y88b.    \n" +
                " \"Y88888 88888P\"  888888888   \"Y88P\"  888     \"Y8888P\n\n";
    }

    @NotNull
    @Override
    Options createOptions() {
        return new Options()
                .addOption("u", "url", true, "JDBC url. (ex:jdbc:oracle:thin@localhost:1521/XE")
                .addOption("l", "login", true, "JDBC login")
                .addOption("p", "password", true, "JDBC password")
                .addOption("o", "orcfile", true, "Orc file. (ex:big-data.orc)")
                .addOption("fs","fetchsize", true, "JDBC fetch size (ex:50)")
                .addOption("h", "help", false, "Print this help")

                .addOption("s", "selectexpr", true, "SELECT-expression (ex: SELECT * FROM EMP)")
                .addOption("t", "tablename", true, "Table or View name")
                .addOption("b", "batchsize", true, "Batch size (rows) default = 50 000")
                .addOption("co", "orc.compression", true, "Orc file compression := { NONE, ZLIB, SNAPPY, LZO, LZ4, ZSTD }")
                .addOption("bc", "orc.bloomcolumns", true, "Orc file bloom filter columns (comma-separated)")
                .addOption("bf", "orc.bloomfilterfpp", true, "False positive probability (float) for bloom filter [0.75..0.99]")
                .addOption("ss", "orc.stripesize", true, "The writer stores the contents of the" +
                                   " stripe in memory until this memory limit is reached and the stripe" +
                                   " is flushed to the HDFS file and the next stripe started")

                .addOption("ri", "orc.rowindexstride", true, "Row index stride [0..1000], 0 - means no index will be.");
    }

    /**
     *
     * <p>Getting results based on a cursor</p>
     *
     * <p>By default the driver collects all the results for the query at once. This can be inconvenient for
     * large data sets so the JDBC driver provides a means of basing a ResultSet on a database cursor and
     * only fetching a small number of rows.</p>
     *
     * <p>A small number of rows are cached on the client side of the connection and when exhausted the next
     * block of rows is retrieved by repositioning the cursor.</p>
     *
     * <p>Note</p>
     *
     *     Cursor based ResultSets cannot be used in all situations. There a number of restrictions which
     *     will make the driver silently fall back to fetching the whole ResultSet at once.
     *
     *     <ul>
     *     <li>The connection to the server must be using the V3 protocol. This is the default for (and is only supported by)
     *       server versions 7.4 and later.
     *     <li>The Connection must not be in autocommit mode. The backend closes cursors at the end of transactions,
     *       so in autocommit mode the backend will have closed the cursor before anything can be fetched from it.
     *     <li>The Statement must be created with a ResultSet type of ResultSet.TYPE_FORWARD_ONLY. This is the default,
     *       so no code will need to be rewritten to take advantage of this, but it also means that you cannot scroll
     *       backwards or otherwise jump around in the ResultSet.
     *     <li>The query given must be a single statement, not multiple statements strung together with semicolons.
     *     </ul>
     *
     */
    public void processWithWriter(@NotNull Writer writer, @NotNull TypeDescription schema, @NotNull Connection connection,
                                  @NotNull String query, int orcBatchSize, int jdbcFetchSize, @NotNull ITypeMapper genericTypeMapper) throws IOException, SQLException {
        logger.traceEntry("processWithWriter orcBatchSize = {}, jdbcFetchSize = {}", orcBatchSize, jdbcFetchSize);

        // TODO: Consider BCEL/Asm to implement table-per-assembly jvm code
        VectorizedRowBatch batch = schema.createRowBatch(orcBatchSize);
        connection.setAutoCommit(false);

        try (Statement statement = connection.createStatement()) {
            logger.info("execute query : {}", query);
            // TODO: Parametrize
            statement.setFetchSize(jdbcFetchSize);
            statement.executeQuery(query);
            // ResultSet.TYPE_FORWARD_ONLY
            try(ResultSet resultSet = statement.getResultSet()) {
                int rows = 0;
                long batches = 0;
                while (resultSet.next()) {
                    genericTypeMapper.toOrcVectorized(batch, rows, resultSet);
                    batch.size++;
                    rows++;
                    if (batch.size >= orcBatchSize) {
                        logger.trace("Add batch # {}", batches);
                        writer.addRowBatch(batch);
                        batches++;
                        batch.reset();
                        rows=0;
                    }
                }
                if (batch.size > 0) {
                    logger.trace("Add batch # {}", batches);
                    writer.addRowBatch(batch);
                    batches++;
                    batch.reset();
                }
                logger.info("Successfully write {} rows and {} batches", rows, batches);
            }
        }
    }

    public void process(@NotNull Properties properties) throws SQLException, IOException {
        logger.info("[1] Start process");

        String url = properties.getProperty("url");

        try (Connection connection = DriverManager.getConnection(
                url,
                properties.getProperty("login"),
                properties.getProperty("password"))) {

            logger.info("[2] Read metadata from DB");

            DatabaseMetaData metadata = connection.getMetaData();

            String tableName = (String) properties.getOrDefault("tablename", null);
            String selectExpr = (String) properties.getOrDefault("selectexpr", null);

            ResultSet resultSetColumns = SQLUtils.getColumns(metadata, tableName);

            ResultSetMetaData resultSetColumnsMetaData = resultSetColumns.getMetaData();

            TypeDescription schema = TypeDescription.createStruct();

            logger.info("[3] Process type mapper");

            ITypeMapper genericTypeMapper = MapperManager.instance.detect(url);

            while (resultSetColumns.next()) {
                String columnName = resultSetColumns.getString("COLUMN_NAME");
                int dataType = resultSetColumns.getInt("DATA_TYPE");
                String typeName = resultSetColumns.getString("TYPE_NAME");
                int nullAllowed = resultSetColumns.getInt("NULLABLE");
                int columnSize = resultSetColumns.getInt("COLUMN_SIZE");

                logger.info("{} : dataType = {}, columnSize = {}, typeName = {}, nullAllowred = {}", columnName, dataType, columnSize, typeName, nullAllowed);
                // TODO: Pass length, precission, nullable
                TypeDescription typeDescription = genericTypeMapper.toOrc(typeName, Optional.of(columnSize), Optional.of(0), true);
                logger.info("typeDescription = {}", typeDescription);
                schema.addField(columnName, typeDescription);
            }

            resultSetColumns.close();

            String orcFilePath = properties.getProperty("orcfile");
            logger.info("[4] Export ORC file = {}", orcFilePath);
            Configuration conf = new Configuration();
            conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
            conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
            String userDir = System.getProperty("user.dir");
            logger.info("[4.3] User dir = {}", userDir);
            org.apache.hadoop.fs.Path currentDirPath = new org.apache.hadoop.fs.Path(userDir);
            logger.info("[5] currentDirPath = {}", currentDirPath);
            org.apache.hadoop.fs.FileSystem currentDirPathFileSystem = currentDirPath.getFileSystem(conf);
            logger.info("[6] fs.canonicalServName = {}", currentDirPathFileSystem.getCanonicalServiceName());
            currentDirPathFileSystem.delete(new Path(orcFilePath), false);
            logger.info("[6.1] create Orc-Writer with schema");

            int batchSize = 1000;
            if (properties.containsKey("batchsize")) {
                batchSize = Integer.parseInt(properties.getProperty("batchsize"));
            }
            int fetchSize = 50;
            if (properties.containsKey("fetchsize")) {
                fetchSize = Integer.parseInt(properties.getProperty("fetchsize"));
            }
            try (Writer writer = OrcUtils.createWriter(currentDirPathFileSystem, orcFilePath, schema, properties)) {
                if (selectExpr != null) {
                    // TODO
                    throw new RuntimeException("Non implemented yet!");
                } else if (tableName != null) {
                    processWithWriter(writer, schema, connection, "SELECT * FROM " + tableName, batchSize, fetchSize, genericTypeMapper);
                } else {
                    throw new IllegalArgumentException("Undefined table or SQL-expression for export!");
                }
            } catch (IOException ex) {
                logger.error("IOException : ", ex);
            }
            logger.info("[6.2] Orc-Writer closed");
        } catch (SQLException ex) {
            logger.error("SQLException : ", ex);
        }
        logger.info("[7] Finish!");
    }

    public void process(String[] args) throws SQLException, ParseException, IOException {

        Properties db2orcProperties = new Properties();
        try {
            db2orcProperties.load(new FileInputStream("db2orc.properties"));
        } catch (IOException ex) {
            logger.warn("Unable to found db2orc.properties");
        }

        Properties properties = new Properties();

        CommandLineParser parser = new DefaultParser();
        Options options = createOptions();
        if (args.length == 0) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(createLogo(), createOptions());
        } else {
            CommandLine line = parser.parse(options, args);
            if (line.hasOption("h")) {
                HelpFormatter formatter = new HelpFormatter();
                formatter.printHelp(createLogo(), createOptions());
                return;
            }
            // Mandatory
            properties.put("url", line.getOptionValue("u"));
            properties.put("login", line.getOptionValue("l"));
            properties.put("password", line.getOptionValue("p"));
            properties.put("orcfile", line.getOptionValue("o"));
            // Optional

            properties.put("batchsize", line.hasOption("b") ? line.getOptionValue("b") : 50_000);

            if (line.hasOption("t"))  properties.put("tablename", line.getOptionValue("t"));
            if (line.hasOption("s"))  properties.put("selectexpr", line.getOptionValue("s"));
            if (line.hasOption("co")) properties.put("orc.compression", line.getOptionValue("co"));
            if (line.hasOption("bf")) properties.put("orc.bloomColumns", line.getOptionValue("bf"));
            if (line.hasOption("ss")) properties.put("orc.stripesize", line.getOptionValue("ss"));
            if (line.hasOption("b"))  properties.put("orc.batchsize", line.getOptionValue("b"));
            if (line.hasOption("fs")) properties.put("fetchsize", line.getOptionValue("fs"));

            process(properties);
        }


    }

    public static void main(String[] args) throws SQLException, ParseException, IOException, ClassNotFoundException {
        System.setProperty("log4j1.compatibility", "true");
        System.setProperty("log4j.configuration", "log4j.properties");
        new Db2Orc().process(args);
    }


}
