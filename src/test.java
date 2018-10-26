

import java.sql.*;

public class test {
    public static void main(String[] args) throws SQLException {
        System.out.println("hello world");
        try{
            Class.forName("com.mysql.jdbc.Driver");

        }catch (ClassNotFoundException e){
            System.out.println("不能加载类");
            e.printStackTrace();
        }
        String url = "jdbc:mysql://localhost:3306/mysql";
        String username ="root";
        String password = "123456";
        try{
            Connection connection = DriverManager.getConnection(url,username,password);
            String sql = "create database lab1";
            Statement stmt = connection.createStatement();
            stmt.execute(sql);
            //如果有数据，rs.next()返回true
//            while(rs.next()){
//                System.out.println(rs.getString("user")+" host: "+rs.getString("host"));
//            }
            stmt.execute("show databases");


        }catch (Exception e){
            System.out.println("数据库连接失败");
            System.out.println(e.getMessage());
        }


    }
}