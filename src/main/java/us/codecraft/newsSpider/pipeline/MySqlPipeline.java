package us.codecraft.newsSpider.pipeline;

import us.codecraft.newsSpider.model.MySqlConnection;
import us.codecraft.newsSpider.model.News;
import us.codecraft.newsSpider.ResultItems;
import us.codecraft.newsSpider.Task;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Map;

public class MySqlPipeline implements Pipeline {

    public MySqlPipeline(){}

    /**
     * Get the result item and store it into the mysql database.
     * @param resultItems resultItems
     * @param task task
     */
    @Override
    public synchronized void process(ResultItems resultItems, Task task) {
        News news = resultItems.get("news");

        Connection conn = MySqlConnection.getConnection();
        try {
            String querySql = "select count(*) from news_info where url=(?)";
            PreparedStatement queryStatement =conn.prepareStatement(querySql);
            queryStatement.setString(1, news.getUrl());
            ResultSet rs = queryStatement.executeQuery();
            rs.next();
            if(Integer.valueOf(rs.getString(1)) < 1) {
                String insertSql = "insert into news_info(kind, url, title, introduction) values (?,?,?,?)";
                PreparedStatement insertStatement = conn.prepareStatement(insertSql);
                insertStatement.setString(1, news.getKind().trim());
                insertStatement.setString(2, news.getUrl().trim());
                insertStatement.setString(3, news.getTitle().trim());
                insertStatement.setString(4, news.getIntroduction().trim().replaceAll("[\r\n]", ""));
                insertStatement.execute();
                System.out.println("Insert to database successfully");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
