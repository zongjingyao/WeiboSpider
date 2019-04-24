#!/usr/bin/env python
# encoding: utf-8
import re
from lxml import etree
from scrapy import Spider
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.utils.project import get_project_settings
from sina.items import TweetsItem, InformationItem, RelationshipsItem, CommentItem
from sina.spiders.utils import time_fix
import time


class WeiboSpider(Spider):
    name = "weibo_spider"
    base_url = "https://weibo.cn"

    def start_requests(self):
        self.user_ids_cache = {}
        start_uids = [
            '2803301701',  # 人民日报
            '1699432410'  # 新华社
        ]
        for uid in start_uids:
            yield Request(url="https://weibo.cn/%s/info" % uid, callback=self.parse_information)

    def parse_information(self, response):
        """ 抓取个人信息 """
        information_item = InformationItem()
        information_item['crawl_time'] = int(time.time())
        selector = Selector(response)
        information_item['_id'] = re.findall('(\d+)/info', response.url)[0]
        text1 = ";".join(selector.xpath('body/div[@class="c"]//text()').extract())  # 获取标签里的所有text()
        nick_name = re.findall('昵称;?[：:]?(.*?);', text1)
        gender = re.findall('性别;?[：:]?(.*?);', text1)
        place = re.findall('地区;?[：:]?(.*?);', text1)
        briefIntroduction = re.findall('简介;?[：:]?(.*?);', text1)
        birthday = re.findall('生日;?[：:]?(.*?);', text1)
        sex_orientation = re.findall('性取向;?[：:]?(.*?);', text1)
        sentiment = re.findall('感情状况;?[：:]?(.*?);', text1)
        vip_level = re.findall('会员等级;?[：:]?(.*?);', text1)
        authentication = re.findall('认证;?[：:]?(.*?);', text1)
        labels = re.findall('标签;?[：:]?(.*?)更多>>', text1)
        if nick_name and nick_name[0]:
            information_item["nick_name"] = nick_name[0].replace(u"\xa0", "")
        if gender and gender[0]:
            information_item["gender"] = gender[0].replace(u"\xa0", "")
        if place and place[0]:
            place = place[0].replace(u"\xa0", "").split(" ")
            information_item["province"] = place[0]
            if len(place) > 1:
                information_item["city"] = place[1]
        if briefIntroduction and briefIntroduction[0]:
            information_item["brief_introduction"] = briefIntroduction[0].replace(u"\xa0", "")
        if birthday and birthday[0]:
            information_item['birthday'] = birthday[0]
        if sex_orientation and sex_orientation[0]:
            if sex_orientation[0].replace(u"\xa0", "") == gender[0]:
                information_item["sex_orientation"] = "同性恋"
            else:
                information_item["sex_orientation"] = "异性恋"
        if sentiment and sentiment[0]:
            information_item["sentiment"] = sentiment[0].replace(u"\xa0", "")
        if vip_level and vip_level[0]:
            information_item["vip_level"] = vip_level[0].replace(u"\xa0", "")
        if authentication and authentication[0]:
            information_item["authentication"] = authentication[0].replace(u"\xa0", "")
        if labels and labels[0]:
            information_item["labels"] = labels[0].replace(u"\xa0", ",").replace(';', '').strip(',')
        request_meta = response.meta
        request_meta['item'] = information_item
        yield Request(self.base_url + '/u/{}'.format(information_item['_id']),
                      callback=self.parse_further_information,
                      meta=request_meta, dont_filter=True, priority=1)

    def parse_further_information(self, response):
        text = response.text
        information_item = response.meta['item']
        tweets_num = re.findall('微博\[(\d+)\]', text)
        if tweets_num:
            information_item['tweets_num'] = int(tweets_num[0])
        follows_num = re.findall('关注\[(\d+)\]', text)
        if follows_num:
            information_item['follows_num'] = int(follows_num[0])
        fans_num = re.findall('粉丝\[(\d+)\]', text)
        if fans_num:
            information_item['fans_num'] = int(fans_num[0])
        yield information_item

        # 获取该用户微博
        yield Request(url=self.base_url + '/{}/profile?page=1'.format(information_item['_id']),
                      callback=self.parse_tweet,
                      priority=1)

        # 获取关注列表
        yield Request(url=self.base_url + '/{}/follow?page=1'.format(information_item['_id']),
                      callback=self.parse_follow,
                      dont_filter=True)
        # 获取粉丝列表
        yield Request(url=self.base_url + '/{}/fans?page=1'.format(information_item['_id']),
                      callback=self.parse_fans,
                      dont_filter=True)

    def parse_tweet(self, response):
        if response.url.endswith('page=1'):
            # 如果是第1页，一次性获取后面的所有页
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_tweet, dont_filter=True, meta=response.meta)
        """
        解析本页的数据
        """
        tree_node = etree.HTML(response.body)
        tweet_nodes = tree_node.xpath('//div[@class="c" and @id]')
        for tweet_node in tweet_nodes:
            try:
                tweet_item = TweetsItem()
                tweet_item['crawl_time'] = int(time.time())
                tweet_repost_url = tweet_node.xpath('.//a[contains(text(),"转发[")]/@href')[0]
                user_tweet_id = re.search(r'/repost/(.*?)\?uid=(\d+)', tweet_repost_url)
                tweet_item['weibo_url'] = 'https://weibo.cn/{}/{}'.format(user_tweet_id.group(2),
                                                                           user_tweet_id.group(1))
                tweet_item['user_id'] = user_tweet_id.group(2)
                tweet_item['_id'] = '{}_{}'.format(user_tweet_id.group(2), user_tweet_id.group(1))
                create_time_info = tweet_node.xpath('.//span[@class="ct"]/text()')[-1]
                if "来自" in create_time_info:
                    tweet_item['created_at'] = time_fix(create_time_info.split('来自')[0].strip())
                else:
                    tweet_item['created_at'] = time_fix(create_time_info.strip())

                like_num = tweet_node.xpath('.//a[contains(text(),"赞[")]/text()')[-1]
                tweet_item['like_num'] = int(re.search('\d+', like_num).group())

                repost_num = tweet_node.xpath('.//a[contains(text(),"转发[")]/text()')[-1]
                tweet_item['repost_num'] = int(re.search('\d+', repost_num).group())

                comment_num = tweet_node.xpath(
                    './/a[contains(text(),"评论[") and not(contains(text(),"原文"))]/text()')[-1]
                tweet_item['comment_num'] = int(re.search('\d+', comment_num).group())

                # divs[0] 原博
                # divs[1] 图片
                # divs[2] 转发理由
                divs = tweet_node.xpath('.//div')
                is_repost = False
                children = divs[-1].getchildren()
                if len(divs) > 1 and children[0].tag == 'span' and children[0].text == '转发理由:':
                    all_content_text = ''
                    is_repost = True
                    for child in children:
                        if child.tag in ['span', 'a'] and child.text and child.text.startswith('赞'):
                            break
                        if child.tag == 'img' and 'emoticon' in child.attrib['src']:
                            all_content_text += child.attrib['alt']
                        elif child.tag == 'a':
                            all_content_text += child.text.strip()
                        if child.tail:
                            all_content_text += child.tail.strip()
                    tweet_item['content'] = all_content_text.strip()

                key = 'original_content' if is_repost else 'content'
                # 检测由没有阅读全文:
                all_content_link = tweet_node.xpath('.//a[text()="全文" and contains(@href,"ckAll=1")]')
                if all_content_link:
                    all_content_url = self.base_url + all_content_link[0].xpath('./@href')[0]
                    yield Request(all_content_url, callback=self.parse_all_content, meta={'item': tweet_item, 'key': key},
                                  priority=1)

                else:
                    all_content_text = ''
                    for child in divs[0].getchildren()[1 if is_repost else 0:]:
                        if child.tag in ['span', 'a'] and child.text and child.text.startswith('赞'):
                            break
                        if child.tag == 'a':
                            break
                        if child.text:
                            all_content_text += child.text.strip()
                        for grandchild in child.getchildren():
                            if grandchild.tail:
                                all_content_text += grandchild.tail.strip()
                            if grandchild.tag == 'img' and 'emoticon' in grandchild.attrib['src']:
                                all_content_text += grandchild.attrib['alt']
                        if child.tail:
                            all_content_text += child.tail.strip()
                    if all_content_text.endswith('['):
                        all_content_text = all_content_text[:-1]
                    tweet_item[key] = all_content_text.strip()
                    yield tweet_item

                # 抓取该微博的评论信息
                comment_url = self.base_url + '/comment/' + tweet_item['weibo_url'].split('/')[-1] + '?page=1'
                yield Request(url=comment_url, callback=self.parse_comment, meta={'weibo_url': tweet_item['weibo_url']})

            except Exception as e:
                self.logger.error(e)

    def parse_all_content(self, response):
        # 有阅读全文的情况，获取全文
        tree_node = etree.HTML(response.body)
        tweet_item = response.meta['item']
        content_node = tree_node.xpath('//*[@id="M_"]/div[1]')[0]
        all_content_text = content_node.xpath('string(.)').split(':', maxsplit=1)[1]
        all_content_text = all_content_text.split('\xa0')[0]
        if ':' in all_content_text:
            all_content_text = all_content_text.split(':')[1]
        tweet_item[response.meta['key']] = all_content_text.strip()
        yield tweet_item

    def parse_follow(self, response):
        """
        抓取关注列表
        """
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_follow, dont_filter=True, meta=response.meta)
        selector = Selector(response)
        urls = selector.xpath('//a[text()="关注他" or text()="关注她" or text()="取消关注"]/@href').extract()
        uids = re.findall('uid=(\d+)', ";".join(urls), re.S)
        ID = re.findall('(\d+)/follow', response.url)[0]
        for uid in uids:
            relationships_item = RelationshipsItem()
            relationships_item['crawl_time'] = int(time.time())
            relationships_item["fan_id"] = ID
            relationships_item["followed_id"] = uid
            relationships_item["_id"] = ID + '-' + uid
            yield relationships_item

    def parse_fans(self, response):
        """
        抓取粉丝列表
        """
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_fans, dont_filter=True, meta=response.meta)
        selector = Selector(response)
        urls = selector.xpath('//a[text()="关注他" or text()="关注她" or text()="移除"]/@href').extract()
        uids = re.findall('uid=(\d+)', ";".join(urls), re.S)
        ID = re.findall('(\d+)/fans', response.url)[0]
        for uid in uids:
            relationships_item = RelationshipsItem()
            relationships_item['crawl_time'] = int(time.time())
            relationships_item["fan_id"] = uid
            relationships_item["followed_id"] = ID
            relationships_item["_id"] = uid + '-' + ID
            yield relationships_item

    def parse_comment(self, response):
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_comment, dont_filter=True, meta=response.meta)
        tree_node = etree.HTML(response.body)
        comment_nodes = tree_node.xpath('//div[@class="c" and contains(@id,"C_")]')
        for comment_node in comment_nodes:
            comment_item = CommentItem()
            comment_item['crawl_time'] = int(time.time())
            comment_item['weibo_url'] = response.meta['weibo_url']
            all_content_text = ''
            for child in comment_node.getchildren():
                if child.tag == 'a' and child.text == '举报':
                    break
                if child.tag == 'img' and 'emoticon' in child.attrib['src']:
                    all_content_text += child.attrib['alt']
                if child.text:
                    all_content_text += child.text.strip()
                if child.tail:
                    all_content_text += child.tail.strip()
                if child.tag == 'span' and child.attrib['class'] == 'ctt':
                    for grandchild in child.getchildren():
                        if grandchild.text:
                            all_content_text += grandchild.text.strip()
                        if grandchild.tail:
                            all_content_text += grandchild.tail.strip()
                        if grandchild.tag == 'img' and 'emoticon' in grandchild.attrib['src']:
                            all_content_text += grandchild.attrib['alt']
            comment_item['content'] = all_content_text.strip()
            comment_item['_id'] = comment_node.xpath('./@id')[0]
            created_at = comment_node.xpath('.//span[@class="ct"]/text()')[0]
            comment_item['created_at'] = time_fix(created_at.split('\xa0')[0])

            href = comment_node.xpath('./a')[0].attrib['href']
            href_parts = href.split('/')
            if len(href_parts) == 3 and href_parts[1] == 'u':
                comment_item['comment_user_id'] = href_parts[2]
            elif href_parts[-1] in self.user_ids_cache.keys():
                comment_item['comment_user_id'] = self.user_ids_cache[href_parts[-1]]
            else:
                page_url = self.base_url + href
                yield Request(page_url, self.parse_commnet_user_id, dont_filter=True, meta={'comment_item':comment_item})
            
            yield comment_item

    def parse_commnet_user_id(self, response):
        tree_node = etree.HTML(response.body)
        follow_node = tree_node.xpath('.//a[contains(text(),"关注") and contains(@href,"/follow")]')[0]
        comment_user_id = re.findall('(\d+)/follow', follow_node.attrib['href'])[0]
        comment_item = response.meta['comment_item']
        comment_item['comment_user_id'] = comment_user_id
        self.user_ids_cache[response.url.split('/')[-1]] = comment_user_id
        yield comment_item

if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl('weibo_spider')
    process.start()
