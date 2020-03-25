package mayton.db;

import mayton.db.pg.PGTypeMapper;
import org.apache.commons.cli.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class Db2Orc extends GenericMainApplication {

    public static Logger logger = LogManager.getLogger(Db2Orc.class);

    private static final boolean DEVMODE = false;

    static String logo =
            "============================================================================================" +
            "     888 888       .d8888b.                           \n" +
            "     888 888      d88P  Y88b                          \n" +
            "     888 888             888                          \n" +
            " .d88888 88888b.       .d88P  .d88b.  888d888 .d8888b \n" +
            "d88\" 888 888 \"88b  .od888P\"  d88\"\"88b 888P\"  d88P\"    \n" +
            "888  888 888  888 d88P\"      888  888 888    888      \n" +
            "Y88b 888 888 d88P 888\"       Y88..88P 888    Y88b.    \n" +
            " \"Y88888 88888P\"  888888888   \"Y88P\"  888     \"Y8888P ";

    @Override
    Options createOptions() {
        return new Options()
                .addOption("u", "url",       true, "JDBC url. (ex:jdbc:oracle:thin@localhost:1521/XE")
                .addOption("l", "login",     true, "JDBC login")
                .addOption("p", "password",  true, "JDBC password")
                .addOption("o", "orcfile",   true, "Orc file. (ex:big-data.orc)")
                .addOption("s", "selectexpr",true, "SELECT-expression")
                .addOption("t", "tablename", true, "Table or View name (excludes select expression)");
    }

    public void process(Properties properties) throws SQLException, ClassNotFoundException, IOException {
        logger.info("[1] Start process");

        Connection connection = DriverManager.getConnection(
                properties.getProperty("url"),
                properties.getProperty("login"),
                properties.getProperty("password"));

        logger.info("[2] Read metadata from DB");

        DatabaseMetaData metadata = connection.getMetaData();

        ResultSet res = metadata.getColumns(null, null, properties.getProperty("tablename"), null);

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
        Writer writer = OrcUtils.createWriter(currentDirPathFileSystem, orcFilePath, schema);
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
            CommandLine line = parser.parse(options, args);
            properties.put("url", line.getOptionValue("u"));
            properties.put("login", line.getOptionValue("l"));
            properties.put("password", line.getOptionValue("p"));
            if (line.hasOption("t")) properties.put("tablename", line.getOptionValue("t"));
            if (line.hasOption("s")) properties.put("selectexpr", line.getOptionValue("s"));
            properties.put("orcfile", line.getOptionValue("o"));
        }
        process(properties);
    }

    public static void main(String[] args) throws SQLException, ParseException, IOException, ClassNotFoundException {
        System.setProperty("log4j1.compatibility", "true");
        System.setProperty("log4j.configuration", "log4j.properties");
        new Db2Orc().process(args);
    }


}
