package us.codecraft.newsSpider.model;

import com.mysql.jdbc.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class MySqlConnection {

    private static Connection conn = null;

    static {
        try{
            String driver = "com.mysql.cj.jdbc.Driver";
            String password = "Zjh19971215";
            String username = "root";
            String url = "jdbc:mysql://47.107.97.154:3306/three_news?useUnicode=true&characterEncoding=utf8";

            Class.forName(driver);
            conn = DriverManager.getConnection(url, username, password);
            System.out.println("Connect to database successfully...");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static Connection getConnection() {
        return conn;
    }
}
