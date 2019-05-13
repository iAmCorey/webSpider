package us.codecraft.newsSpider.example;

import org.junit.Test;
import us.codecraft.newsSpider.ResultItems;
import us.codecraft.newsSpider.Spider;
import us.codecraft.newsSpider.Task;
import us.codecraft.newsSpider.downloader.MockGithubDownloader;
import us.codecraft.newsSpider.pipeline.Pipeline;
import us.codecraft.newsSpider.processor.example.GithubRepoPageProcessor;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * @author code4crafter@gmail.com
 *         Date: 16/1/19
 *         Time: 上午7:27
 */
public class GithubRepoPageProcessorTest {

    @Test
    public void test_github() throws Exception {
        Spider.create(new GithubRepoPageProcessor()).addPipeline(new Pipeline() {
            @Override
            public void process(ResultItems resultItems, Task task) {
                assertThat(((String) resultItems.get("name")).trim()).isEqualTo("newsSpider");
                assertThat(((String) resultItems.get("author")).trim()).isEqualTo("code4craft");
            }
        }).setDownloader(new MockGithubDownloader()).test("https://github.com/code4craft/webmagic");
    }
}
