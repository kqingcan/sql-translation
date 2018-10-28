
import com.csvreader.CsvReader;
import sun.plugin2.os.windows.Windows;

import java.io.*;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.sql.*;
import java.util.Scanner;

public class DataTransporter {
    private String url = "jdbc:mysql://localhost:3306/mysql?characterEncoding=UTF-8";
    private String username = "root";
    private String password = "123456";

    public DataTransporter(){
        loadMysqlDriver();
    }

    public DataTransporter(String url, String username, String password){
        this.url = url;
        this.username = username;
        this.password = password;
        loadMysqlDriver();
    }

    private void loadMysqlDriver(){
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

        } catch (ClassNotFoundException e) {
            System.out.println("不能加载mysql驱动类");
            e.printStackTrace();
        }
    }

    private Connection connectToMysql(){
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(this.url, this.username, this.password);

        } catch (Exception e) {
            System.out.println("数据库连接失败");
            System.out.println(e.getMessage());
        }
        return connection;
    }

    private Connection loadSQLiteDriver(String filename){
        Connection connection = null;
        try {
            Class.forName("org.sqlite.JDBC");
            connection = DriverManager.getConnection("jdbc:sqlite:databases/"+filename);
        } catch (Exception e) {
            System.out.println("sqlite数据库连接错误！");
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        return connection;
    }

    private CsvReader readDataFromFile(String filename){
        CsvReader csvReader = null;
        try {
            csvReader = new CsvReader(new FileInputStream("databases/"+filename), Charset.forName("utf-8"));
            csvReader.readHeaders();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return csvReader;
    }

    private StringBuilder insertFromCsv(Connection connection, String filename) throws IOException, SQLException {
        CsvReader csvReader1 = readDataFromFile(filename);
        String headers[] = csvReader1.getHeaders();
        String table = filename.split("\\.")[0];
        String insertSQL = generateInsertSQL(headers,table);
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

    private StringBuilder insertFromSQLite(Connection connection, String filename) throws SQLException {
        Connection sqliteConnection = loadSQLiteDriver(filename);
        Statement statement = sqliteConnection.createStatement();
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
            int length = rm.getColumnCount();
            String headers[] = new String[length];
            for (int i=0;i<length;i++){
                headers[i] = rm.getColumnName(i+1);
            }
            String insertSQL = generateInsertSQL(headers,table);
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

    private String generateInsertSQL(String[] headers, String table){
        String insertSQL = "INSERT INTO "+ table +"(";
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

        return insertSQL;
    }

    private void interactiveInTerminal(Connection connection){
        try {
            String createDatabase = "create database if not exists lab1";
            Statement  stmt = null;
            Scanner input = new Scanner(System.in);
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
                        StringBuilder temp1= insertFromCsv(connection,csvFilename);
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


    }

    public void run(){
        Connection connection = connectToMysql();
        interactiveInTerminal(connection);
    }
}