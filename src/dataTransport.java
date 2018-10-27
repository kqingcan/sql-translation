
import com.csvreader.CsvReader;
import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;
import java.util.Scanner;

public class dataTransport {
    public static void main(String[] args) throws SQLException, IOException {
        System.out.println("hello world");
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

        } catch (ClassNotFoundException e) {
            System.out.println("不能加载类");
            e.printStackTrace();
        }
        String url = "jdbc:mysql://localhost:3306/mysql?characterEncoding=UTF-8";
        String username = "root";
        String password = "123456";
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(url, username, password);

        } catch (Exception e) {
            System.out.println("数据库连接失败");
            System.out.println(e.getMessage());
        }
        String sql = "create database if not exists lab1";
        Statement stmt = connection.createStatement();
        stmt.execute(sql);
        stmt.execute("use lab1");
        Scanner scanner = new Scanner(new File("createTable.txt"));
        while (scanner.hasNext()) {
            String temp = scanner.nextLine();
            System.out.println(temp);
            stmt.execute(temp);
        }
//        String filename = "room.csv";
//        insertToMysql(connection,filename);
        insertFromSQLite(connection, "xxxdatabase.db");
    }

    public static CsvReader readDataFromFile(String filename) throws IOException {
        CsvReader csvReader = null;
        try {
            // 创建CSV读对象
            csvReader = new CsvReader(new FileInputStream(filename), Charset.forName("utf-8"));
            csvReader.readHeaders();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return csvReader;

    }

    public static void insertToMysql(Connection connection, String filename) throws IOException, SQLException {
        CsvReader csvReader1 = readDataFromFile(filename);
        String headers[] = csvReader1.getHeaders();
        String table = filename.split("\\.")[0];
        String insertSQL = "INSERT INTO " + table + "(";
        for (int i = 0; i < headers.length; i++) {
            String head = headers[i];
            if (i == 0) insertSQL = insertSQL + head;
            else insertSQL = insertSQL + "," + head;
        }
        insertSQL = insertSQL + ") VALUES (";
        for (int i = 0; i < headers.length; i++) {
            if (i == 0) insertSQL = insertSQL + "?";
            else insertSQL = insertSQL + ", ?";
        }
        insertSQL = insertSQL + ")";
        System.out.println(insertSQL);

        PreparedStatement preInsert = connection.prepareStatement(insertSQL);
        while (csvReader1.readRecord()) {
            for (int i = 0; i < headers.length; i++) {
                preInsert.setString(i + 1, csvReader1.get(headers[i]));
            }
            preInsert.execute();
        }
    }

    public static void insertFromSQLite(Connection connection, String filename) throws SQLException {
        Connection c = null;
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:xxxdatabase.db");
        } catch (Exception e) {
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        Statement statement = c.createStatement();
        String getTables = "SELECT tbl_name FROM sqlite_master";
        List<String> tableList = new ArrayList();
        ResultSet tables = statement.executeQuery(getTables);
        while (tables.next()) {
            String tableTemp = tables.getString("tbl_name");
            tableList.add(tableTemp);
        }
        for (String table : tableList) {
            String sqlTemp = "SELECT * FROM " + table;
            ResultSet resTemp = statement.executeQuery(sqlTemp);
            ResultSetMetaData rm = resTemp.getMetaData();
            String insertSQL = "INSERT INTO " + table + "(";

            for (int i = 0; i < rm.getColumnCount(); i++) {
                String keyTemp = rm.getColumnName(i + 1);
                if (i == 0) insertSQL = insertSQL + keyTemp;
                else insertSQL = insertSQL + ", " + keyTemp;
            }
            insertSQL = insertSQL + ") VALUES(";
            for (int i = 0; i < rm.getColumnCount(); i++) {
                if (i == 0) insertSQL = insertSQL + "?";
                else insertSQL = insertSQL + ", ?";
            }
            insertSQL = insertSQL + ")";
            PreparedStatement preInsert = connection.prepareStatement(insertSQL);
            StringBuilder sql = new StringBuilder();
            while (resTemp.next()) {
                for (int i = 0; i < rm.getColumnCount(); i++) {
                    preInsert.setString(i + 1, resTemp.getString(i + 1));
                }
//                System.out.println(preInsert);
                if (!preInsert.execute()) {
                    sql.append(preInsert).append("\n");
                }

            }
            System.out.println(sql);
        }


    }
}