
import com.csvreader.CsvReader;

import javax.lang.model.element.NestingKind;
import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;
import java.util.Scanner;

public class dataTransport {
    public static void main(String[] args){

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
        String createDatabase = "create database if not exists lab1";
        Statement  stmt =null;
        Scanner input = new Scanner(System.in);
        try {
            stmt = connection.createStatement();
            stmt.execute(createDatabase);
            stmt.execute("use lab1");
            System.out.println("请输入初始化表的文件名称：");
            String initTableFile = input.nextLine();
            Scanner scanner = new Scanner(new File(initTableFile));
            while (scanner.hasNext()) {
                String temp = scanner.nextLine();
                System.out.println(temp);
                stmt.execute(temp);
            }
            System.out.println("数据库表格初始化完成。");

            do {
                System.out.println("清选择导入mysql数据库的方式：");
                System.out.println("1、从csv文件读入；");
                System.out.println("2、从SQlite数据库导入");
                System.out.println("3: 退出");
                String choice = input.nextLine();
                switch (choice){
                    case "1":
                        System.out.println("请输入csv文件名：");
                        String csvFilename = input.nextLine();
                        StringBuilder temp1= insertToMysql(connection,csvFilename);
                        System.out.println("数据导入成功！其中重复的有：");
                        System.out.println(temp1);
                        break;
                    case "2":
                        System.out.println("请输入sqlite数据库文件名称：");
                        String sqliteFilename = input.nextLine();
                        StringBuilder temp2= insertFromSQLite(connection,sqliteFilename);
                        System.out.println("数据导入成功！其中重复的有：");
                        System.out.println(temp2);
                        break;
                    case "3":
                        System.exit(0);
                    default:
                        break;
                }

            }while (true);

        }catch (SQLException e){
            System.out.println(e.getMessage());
        }catch (IOException e){
            System.out.println(e.getMessage());
        }

//        String filename = "room.csv";
//        insertToMysql(connection,filename);
//        insertFromSQLite(connection, "xxxdatabase.db");
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

    public static StringBuilder insertToMysql(Connection connection, String filename) throws IOException, SQLException {
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
        StringBuilder dupliteSql = new StringBuilder();
        while (csvReader1.readRecord()) {
            for (int i = 0; i < headers.length; i++) {
                preInsert.setString(i + 1, csvReader1.get(headers[i]));
            }
            try {
                preInsert.execute();
            }catch (SQLException e){
                dupliteSql.append(preInsert).append("\n");
            }
        }
        return dupliteSql;
    }

    public static StringBuilder insertFromSQLite(Connection connection, String filename) throws SQLException {
        Connection c = null;
        try {
            Class.forName("org.sqlite.JDBC");
            c = DriverManager.getConnection("jdbc:sqlite:"+filename);
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
        StringBuilder dupliteSql = new StringBuilder();
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

            while (resTemp.next()) {
                for (int i = 0; i < rm.getColumnCount(); i++) {
                    preInsert.setString(i + 1, resTemp.getString(i + 1));
                }
                try {
                    preInsert.execute();
                }catch (SQLException e){
                    dupliteSql.append(preInsert).append("\n");
                }


            }

        }
        return dupliteSql;
    }
}