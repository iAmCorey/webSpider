package us.codecraft.newsSpider.processor.example;

import us.codecraft.newsSpider.Page;
import us.codecraft.newsSpider.Site;
import us.codecraft.newsSpider.Spider;
import us.codecraft.newsSpider.pipeline.FilePipeline;
import us.codecraft.newsSpider.processor.PageProcessor;

/**
 * @author code4crafter@gmail.com <br>
 * @since 0.3.2
 */
public class GithubRepoPageProcessor implements PageProcessor {

    private Site site = Site.me().setRetryTimes(3).setSleepTime(1000).setTimeOut(10000);


    @Override
    public void process(Page page) {
        page.addTargetRequests(page.getHtml().links().regex("(https://github\\.com/[\\w\\-]+/[\\w\\-]+)").all());
        page.addTargetRequests(page.getHtml().links().regex("(https://github\\.com/[\\w\\-])").all());
        page.putField("author", page.getUrl().regex("https://github\\.com/(\\w+)/.*").toString());
        page.putField("name", page.getHtml().xpath("//h1[@class='public']/strong/a/text()").toString());
        if (page.getResultItems().get("name")==null){
            //skip this page
            page.setSkip(true);
        }
        page.putField("readme", page.getHtml().xpath("//div[@id='readme']/tidyText()"));
    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        Spider.create(new GithubRepoPageProcessor()).addUrl("https://github.com/code4craft").addPipeline(new FilePipeline("/Users/Personal/Desktop/spiderData/")).thread(5).run();
    }
}
