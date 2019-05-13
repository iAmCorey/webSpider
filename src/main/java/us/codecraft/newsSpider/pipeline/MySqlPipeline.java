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

    private String querySql = "select count(*) from news_info where url=(?)";
    private String insertSql = "insert into news_info(kind, url, title, introduction) values (?,?,?,?)";

    public MySqlPipeline(){}

    @Override
    public synchronized void process(ResultItems resultItems, Task task) {
        News news = resultItems.get("news");

        Connection conn = MySqlConnection.getConnection();
        try {
            PreparedStatement queryStatement =conn.prepareStatement(querySql);
            queryStatement.setString(1, news.getUrl());
            ResultSet rs = queryStatement.executeQuery();
            rs.next();
            if(Integer.valueOf(rs.getString(1)) < 1) {
                PreparedStatement insertStatement = conn.prepareStatement(insertSql);
                insertStatement.setString(1, news.getKind());
                insertStatement.setString(2, news.getUrl());
                insertStatement.setString(3, news.getTitle());
                insertStatement.setString(4, news.getIntroduction());
                insertStatement.execute();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }


    }
}
