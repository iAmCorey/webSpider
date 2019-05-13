package us.codecraft.newsSpider.downloader;

import org.apache.commons.io.IOUtils;
import us.codecraft.newsSpider.Page;
import us.codecraft.newsSpider.Request;
import us.codecraft.newsSpider.Task;
import us.codecraft.newsSpider.selector.PlainText;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author code4crafter@gmail.com
 */
public class MockGithubDownloader implements Downloader {

    @Override
    public Page download(Request request, Task task) {
        Page page = new Page();
        InputStream resourceAsStream = this.getClass().getResourceAsStream("/html/mock-github.html");
        try {
            page.setRawText(IOUtils.toString(resourceAsStream));
        } catch (IOException e) {
            e.printStackTrace();
        }
        page.setRequest(new Request("https://github.com/code4craft/webmagic"));
        page.setUrl(new PlainText("https://github.com/code4craft/webmagic"));
        return page;
    }

    @Override
    public void setThread(int threadNum) {
    }
}
