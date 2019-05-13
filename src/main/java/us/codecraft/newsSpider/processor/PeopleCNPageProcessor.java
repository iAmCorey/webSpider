package us.codecraft.newsSpider.processor;

import us.codecraft.newsSpider.*;
import us.codecraft.newsSpider.model.News;
import us.codecraft.newsSpider.pipeline.ConsolePipeline;
import us.codecraft.newsSpider.pipeline.MySqlPipeline;

public class PeopleCNPageProcessor implements PageProcessor{

    private static final String SiteURL = "http://politics.people.com.cn"; // politics

    // settings - encoding, spider time slot, retry times...
    private Site site = Site.me().setRetryTimes(3).setSleepTime(1000).setDomain(SiteURL);

    /**
     * Spi the article pages.
     * @param page page
     */
    @Override
    public void process(Page page) {
        News news = new News("时政");

        // match article page
        if (page.getUrl().regex("http://politics\\.people\\.com\\.cn/n1/2019/[0-9]{4}/c[0-9]{4}-[0-9]{8}\\.html").match()) {
            news.setUrl(page.getUrl().toString().trim());
            news.setTitle(page.getHtml().xpath("/html/body/div[4]/h1/text()").toString().trim());
            String intro = page.getHtml().xpath("//*[@id=\"rwb_zw\"]/p[1]/text()").toString();
            if (intro == null || intro.trim().equals("") || intro.trim().length() < 10){
                intro = page.getHtml().xpath("//*[@id=\"rwb_zw\"]/p[2]/text()").toString();
            }
            if (intro == null || intro.trim().equals("") || intro.trim().length() < 10){
                intro = page.getHtml().xpath("//*[@id=\"rwb_zw\"]/p[2]/span/text()").toString();
            }
            news.setIntroduction(intro.trim());
        } else { // match article list
            page.addTargetRequests(page.getHtml().xpath("/html/body/div[10]/h1/a/@href").all());
            page.addTargetRequests(page.getHtml().xpath("/html/body/div[11]/div[2]/div/h4/a/@href").all());
            page.addTargetRequests(page.getHtml().xpath("/html/body/div[11]/div[2]/div/ul/li/a/@href").all());
        }

        if(news.getUrl() != null && !news.getUrl().equals("")){
            page.putField("news", news);
        }else{
            page.setSkip(true);
        }
    }

    @Override
    public Site getSite() {
        return site;
    }

    public static void main(String[] args) {
        System.out.println("Starting...");
        Spider.create(new PeopleCNPageProcessor()).addUrl(SiteURL).addPipeline(new MySqlPipeline()).addPipeline(new ConsolePipeline()).thread(5).run();
        System.out.println("End.");
    }
}
