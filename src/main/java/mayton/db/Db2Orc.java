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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.Properties;

public class Db2Orc extends GenericMainApplication {

    public static Logger logger = LogManager.getLogger(Db2Orc.class);

    private static final boolean DEVMODE = true;

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
        Class.forName("org.postgresql.Driver");

        String url = properties.getProperty("url");

        Connection connection = DriverManager.getConnection(
                url,
                properties.getProperty("user"),
                properties.getProperty("password"));

        DatabaseMetaData metadata = connection.getMetaData();

        ResultSet res = metadata.getColumns(null, null, properties.getProperty("tablename"), null);

        TypeDescription schema = TypeDescription.createStruct();

        /*String query = "SELECT column_name, data_type, is_nullable, character_maximum_length\n" +
                " FROM\n" +
                "        information_schema.columns\n" +
                " WHERE\n" +
                "        table_schema = current_schema()\n" +
                "        AND table_name = '" + tablename + "'\n" +
                "        ORDER BY ordinal_position;";*/

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


        String pathString = properties.getProperty("orcfile");
        Path pathObject = new Path(pathString);
        Configuration conf = new Configuration();
        FileSystem fs = new Path(".").getFileSystem(conf);
        fs.delete(pathObject, false);
        Writer writer = OrcUtils.createWriter(fs, pathString, schema);

        writer.close();

        connection.close();
    }

    public void process(String[] args) throws SQLException, ParseException, IOException, ClassNotFoundException {

        Properties properties = new Properties();
        if (DEVMODE) {
            properties.load(new FileInputStream("sensitive.properties"));
        } else {
            CommandLineParser parser = new DefaultParser();
            Options options = createOptions();
            CommandLine line = parser.parse(options, args);
            String url       = line.getOptionValue("u");
            String user      = line.getOptionValue("l");
            String password  = line.getOptionValue("p");
            String tablename = line.getOptionValue("t");
        }
        process(properties);


    }

    public static void main(String[] args) throws SQLException, ParseException, IOException, ClassNotFoundException {
        System.setProperty("log4j1.compatibility", "true");
        System.setProperty("log4j.configuration", "log4j.properties");
        new Db2Orc().process(args);
    }


}
