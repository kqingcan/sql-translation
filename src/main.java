public class main {

    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/mysql?characterEncoding=UTF-8&&serverTimezone=UTC";
        String username = "root";
        String password = "123456";
        DataTransporter dataTransporter = new DataTransporter(url,username,password);
        dataTransporter.run();
    }
}
