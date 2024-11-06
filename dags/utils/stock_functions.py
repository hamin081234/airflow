import pendulum
import requests
from bs4 import BeautifulSoup
from datetime import datetime

KST = pendulum.timezone("Asia/Seoul")
chrom_ua = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.103 Safari/537.36"


def get_stock_price(company_code, stock_index="NASDAQ"):
    url = f"https://www.google.com/finance/quote/{company_code}:{stock_index}"
    response = requests.get(url, headers={'User-Agent': chrom_ua})
    soup = BeautifulSoup(response.content, 'html.parser')

    stock_name = soup.find('div', {'class': "PdOqHc"})
    for s in stock_name.select('a'):
        s.extract()
    stock_name = stock_name.text.split(" ")[0]

    stock_price_txt = soup.find('div', {'class': "YMlKec fxKbKc"}).text

    stock_currency = stock_price_txt[0]
    stock_price = float(stock_price_txt[1:].replace(",", ""))

    return {
        "url": url,
        "stock_name": stock_name,
        "stock_price": stock_price,
        "currency": stock_currency,
    }


def get_aitimes_news(keyword, include_content=False):
    base_url = "https://www.aitimes.com"
    url = f"https://www.aitimes.com/news/articleList.html?page=1&total=415&box_idxno=&sc_area=A&view_type=tm&sc_word={keyword}"
    response = requests.get(url, headers={'User-Agent': chrom_ua})
    soup = BeautifulSoup(response.content, 'html.parser')

    url_list = []

    for news in soup.find("ul", {"class": "type3 type-Pm pc"}).find_all('li'):
        suffix = news.find("a", href=True)['href']
        news_url = base_url + suffix

        news_response = requests.get(news_url, headers={'User-Agent': chrom_ua})
        news_soup = BeautifulSoup(news_response.content, 'html.parser')

        title = news_soup.find("h3", {"class": "heading"}).get_text(strip=True).replace(u'\xa0', u' ')
        publish_date_txt = news_soup.find("ul", {"class": "infomation"}).find_all("li")[1].text
        publish_date = datetime.strptime(publish_date_txt, " 입력 %Y.%m.%d %H:%M")

        news_info = {
            "url": base_url + suffix,
            "title": title,
            "publish_date": publish_date.strftime("%Y-%m-%d %H:%M"),
        }

        if include_content:
            content_soup = news_soup.find("article", {"itemprop": "articleBody"})
            for div_block in content_soup.select("div"):
                div_block.extract()
            content = content_soup.get_text(strip=True).replace(u'\xa0', u' ')
            news_info["content"] = content

        url_list.append(news_info)

        if len(url_list) >= 5: break

    return url_list


def get_kor_economy_news(keyword, include_content=False):
    base_url = "https://www.hankyung.com/"
    query_url = f"https://search.hankyung.com/search/news?query={keyword}"

    response = requests.get(query_url, headers={'User-Agent': chrom_ua})
    soup = BeautifulSoup(response.content, 'html.parser')
    url_list = []

    for news in soup.find("ul", {"class": "article"}).find_all('li'):
        try:
            url_block = news.find("a", href=True)
            news_url = url_block['href']
            if url_block.find('img', {"class": "icon--member"}) is None:
                news_response = requests.get(news_url, headers={'User-Agent': chrom_ua})
                news_soup = BeautifulSoup(news_response.content, 'html.parser')

                title_soup = news_soup.find("h1", {"class": "headline"})
                if title_soup is None: continue  # 기존에 알고 있는 구조의 기사가 아니라면 패스

                title = title_soup.get_text(strip=True).replace(u'\xa0', u' ')
                publish_date_txt = \
                news_soup.find("div", {"class": "article-timestamp"}).find("div", {"class": "datetime"}).find_all("span", {
                    "class": "txt-date"})[0].text
                publish_date = datetime.strptime(publish_date_txt, "%Y.%m.%d %H:%M")

                news_info = {
                    "url": news_url,
                    "title": title,
                    "publish_date": publish_date.strftime("%Y-%m-%d %H:%M"),
                }

                if include_content:
                    content_soup = news_soup.find("div", {"class": "article-body-wrap"})
                    for figure_blocks in content_soup.select("figure.article-figure"):
                        figure_blocks.extract()
                    for ad_blocks in content_soup.select("div.ad-wrap"):
                        ad_blocks.extract()
                    content = content_soup.get_text(strip=True).replace(u'\xa0', u' ')
                    news_info["content"] = content
                url_list.append(news_info)
        except Exception as e:
            print(e)
            print("html 구조가 달라서 인식이 안됨")
            continue

        if len(url_list) >= 5: break

    return url_list


def get_currency_exchange_rate(from_currency, to_currency):
    """
    using google finance
    :param from_currency:
    :param to_currency:
    :return:
    """
    url = f"https://www.google.com/finance/quote/{from_currency}-{to_currency}"
    req_obj = requests.get(url, headers={'User-Agent': chrom_ua})
    soup = BeautifulSoup(req_obj.content)

    exchange_rate = float(soup.find("div", {"class": "YMlKec fxKbKc"}).text.replace(',', ''))
    time = datetime.now(KST).strftime("%Y-%m-%d %H:%M:%S")

    result = {
        "url": url,
        "From": from_currency,
        "To": to_currency,
        "Exchange Rate": exchange_rate,
        "Time": time,
    }
    return result


