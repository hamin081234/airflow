import pendulum
from datetime import datetime

from utils.stock_functions import get_stock_price
from utils.stock_functions import get_currency_exchange_rate
from utils.stock_functions import get_aitimes_news
from utils.stock_functions import get_kor_economy_news

from airflow.providers.slack.operators.slack import SlackAPIPostOperator

KST = pendulum.timezone("Asia/Seoul")


# Task 0
def get_stock_list():
    stock_list = [
        {
            "code": "TSLA",
            "name_en": "Tesla",
            "name_kr": "테슬라",
            "stock_index": "NASDAQ"
        },
        {
            "code": "AAPL",
            "name_en": "Apple",
            "name_kr": "애플",
            "stock_index": "NASDAQ"
        },
        {
            "code": "NVDA",
            "name_en": "NVidia",
            "name_kr": "엔비디아",
            "stock_index": "NASDAQ"
        },
        {
            "code": "MSFT",
            "name_en": "Microsoft",
            "name_kr": "마이크로소프트",
            "stock_index": "NASDAQ"
        },
    ]
    return stock_list


# Task 1
def all_stock_price(**kwargs):
    stock_list = kwargs['ti'].xcom_pull(task_ids='stock_list')

    for stock in stock_list:
        stock_info = get_stock_price(stock['code'], stock['stock_index'])
        stock["stock_price"] = stock_info["stock_price"]
        stock["stock_info"] = stock_info

    print("#"*50)
    print(stock_list)
    print("#"*50)
    return stock_list


# Task 2
def all_exchange_rate(**kwargs):
    stock_list = kwargs['ti'].xcom_pull(task_ids='stock_price')

    currency_info = get_currency_exchange_rate("USD", "KRW")

    for stock in stock_list:
        stock['to_won'] = currency_info['Exchange Rate'] * stock['stock_price']

    print("#"*50)
    print(currency_info)
    print("#"*50)

    return {"currency_info": currency_info, "stock_list": stock_list}


# Task 3
def all_aitimes_news(**kwargs):
    stock_list = kwargs['ti'].xcom_pull(task_ids='stock_price')

    aitimes_articles = [get_aitimes_news(stock["name_kr"], include_content=True)
                        for stock in stock_list]
    print("#"*50)
    print(aitimes_articles)
    print("#"*50)
    return aitimes_articles


# Task 4
def all_hankyoung_news(**kwargs):
    stock_list = kwargs['ti'].xcom_pull(task_ids='stock_price')

    hankyoung_articles = [get_kor_economy_news(stock["name_kr"], include_content=True)
                          for stock in stock_list]
    print("#"*50)
    print(hankyoung_articles)
    print("#"*50)
    return hankyoung_articles


# Task 5
def send_slack_message(**kwargs):
    infos = kwargs['ti'].xcom_pull(task_ids='exchange_rate')
    stock_list = infos["stock_list"]
    currency_info = infos["currency_info"]
    hankyoung_articles = kwargs['ti'].xcom_pull(task_ids='hankyoung_news')
    aitimes_articles = kwargs['ti'].xcom_pull(task_ids='aitimes_news')

    blocks = []

    header = f"오늘의 주식 현황 ({datetime.now(KST).strftime('%Y-%m-%d %H:%M:%S')})"
    blocks.append({
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": header,
        },
    })

    exchange_rate_message = f"""
        오늘의 환율 ({currency_info["Time"]})
        1 {currency_info["From"]} = *{currency_info["Exchange Rate"]} {currency_info["To"]}*
    """

    blocks.append({
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": exchange_rate_message
        }
    })

    for stock, h_news, a_news in zip(stock_list, hankyoung_articles, aitimes_articles):
        blocks.append({"type": "divider"})

        stock_message = f"""*{stock["name_kr"]}* (*{stock["name_en"]}* / {stock["stock_index"]}:{stock["code"]})\n주가: *{stock["stock_price"]:.2f} USD (= {stock["to_won"]:.2f} KRW)*\n"""

        ai_news = "*AI타임즈 뉴스*\n"
        for idx, news in enumerate(a_news):
            ai_news += f"""• <{news["url"]}|{news["title"][:30]+" ..." if len(news["title"]) > 30 else news["title"]}> ({news["publish_date"]}) \n"""

        hk_news = "*한국경제 뉴스*\n"
        for idx, news in enumerate(h_news):
            hk_news += f"""• <{news["url"]}|{news["title"][:30]+" ..." if len(news["title"]) > 30 else news["title"]}> ({news["publish_date"]}) \n"""

        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": stock_message + ai_news + hk_news
            }
        })

    stock_slack = SlackAPIPostOperator(
        task_id='stock_message',
        channel='#주식',
        blocks=blocks
    )
    stock_slack.execute(context=kwargs)

