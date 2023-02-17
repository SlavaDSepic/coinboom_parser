import httpx
from bs4 import BeautifulSoup
from fake_useragent import UserAgent
import pars_db as db
import time
import google_sheets as goosh
from tqdm import tqdm
import concurrent.futures
import logging
import pycron


logging.basicConfig(
    level=logging.INFO,
    filename="parser_log.log",
    format="%(asctime)s - %(module)s - %(levelname)s - %(funcName)s: %(lineno)d - %(message)s",
    datefmt='%H:%M:%S',
    )

url_marketcap = 'https://coinboom.net/trending'
url_daily_gainers = 'https://coinboom.net/gainers'
url_new_coins = 'https://coinboom.net/new'
url_hot = 'https://coinboom.net/trade'
url_promoted = 'https://coinboom.net/trending'
headers = {'User-Agent': UserAgent().chrome}
session = httpx.Client()
session.headers = headers


def get_page(url):
    responce = session.get(url, timeout=10)
    return responce


def get_number_of_pages(url) -> int:
    max_page = 210
    temp_url = f'{url}?page={max_page}'
    soup = BeautifulSoup(get_page(temp_url).text, 'lxml')
    next_prev = len(soup.find('div', class_='pagination_div').find_all('a'))
    while next_prev == 1:
        max_page -= 1
        new_url = f'{url}?page={max_page}'
        soup = BeautifulSoup(get_page(new_url).text, 'lxml')
        next_prev = len(soup.find('div', class_='pagination_div').find_all('a'))
    number_of_pages = max_page + 1
    return number_of_pages


def get_coins_urls(page_url, promoted) -> list:
    responce = session.get(page_url)
    soup = BeautifulSoup(responce.text, 'lxml')
    if promoted:
        page_coins_urls = set([f"https://coinboom.net{tag.get('href').lstrip('.')}" for tag in soup.find_all(id='toplist')[0].find_all('a') if tag.get('href').startswith('./coin/')])
    else:
        page_coins_urls = set([f"https://coinboom.net{tag.get('href').lstrip('.')}" for tag in soup.find_all(id='toplist')[1].find_all('a') if tag.get('href').startswith('./coin/')])
    return list(page_coins_urls)


def get_all_coins_urls(start_url, promoted) -> list:
    if promoted:
        return get_coins_urls(start_url, promoted)
    else:
        number_of_pages = get_number_of_pages(start_url)
    all_coins_urls = []
    for i in range(1, number_of_pages+1):
        page_url = f'{start_url}?page={i}'
        try:
            all_coins_urls.extend(get_coins_urls(page_url, promoted))
        except Exception as ex:
            logging.exception(f'{ex} --- {page_url}')
    return all_coins_urls



def parse_coin_page(coin_url):
    try:
        responce = get_page(coin_url)
    except Exception as ex:
        logging.exception(f'{ex} --- {coin_url}')
        return None
    soup = BeautifulSoup(responce.text, 'lxml')
    try:
        coin_names = soup.find('h1', class_='nested_header').text
        coin_short_name = coin_names.split('(')[-1].split(')')[0]
        coin_name = coin_names.split('(')[0]
    except Exception as ex:
        logging.exception(f'{ex} --- {coin_url}')
        return None
    socials_links = list(soup.find('div', class_='contact_div').children)[-5]
    socials_links = [a.get('href') for a in socials_links.find_all('a')]
    coin_domain = socials_links[0]
    coin_domains_other = ''
    telegram = ''
    twitter = ''
    facebook = ''
    discord = ''
    reddit = ''
    linkedin = ''
    bitcointalk = ''
    medium = ''
    instagram = ''
    youtube = ''
    tiktok = ''
    other_social_links = []
    for link in socials_links[1:]:
        if 't.me/' in link:
            telegram = link
        elif 'twitter' in link:
            twitter = link
        elif 'facebook.com/' in link:
            facebook = link
        elif 'discord' in link:
            discord = link
        elif 'reddit.com/' in link:
            reddit = link
        elif 'linkedin.com' in link:
            linkedin = link
        elif 'bitcointalk' in link:
            bitcointalk = link
        elif 'medium.com' in link:
            medium = link
        elif 'instagram.com' in link:
            instagram = link
        elif 'youtube.com/' in link:
            youtube = link
        elif 'tiktok.com/' in link:
            tiktok = link
        else:
            other_social_links.append(link)
    try:
        coin_description = soup.find('div', class_='detail_description').text.strip()
    except Exception:
        coin_description = 'No description'
    try:
        coin_audit = soup.find('p', text='Audited').parent.find('a').get('href')
    except Exception:
        coin_audit = 'Token Audit unknown'
    coin_listing_status = 'No info'
    try:
        coin_launch = list(soup.find('div', class_='contact_div').children)[-3].text
        coin_presale_status = ''
    except Exception:
        coin_launch = ''
        coin_presale_status = ''
    coin = {'coin_name': coin_name,
            'coin_short_name': coin_short_name,
            'coin_url': coin_url,
            'coin_domain': coin_domain,
            'coin_domains_other': coin_domains_other,
            'telegram': telegram,
            'twitter': twitter,
            'facebook': facebook,
            'discord': discord,
            'reddit': reddit,
            'linkedin': linkedin,
            'bitcointalk': bitcointalk,
            'medium': medium,
            'instagram': instagram,
            'youtube': youtube,
            'tiktok': tiktok,
            'other_social_links': '\n'.join(other_social_links),
            'coin_description': coin_description,
            'coin_audit': coin_audit,
            'coin_listing_status': coin_listing_status,
            'coin_launch': coin_launch,
            'coin_presale_status': coin_presale_status
            }
    return coin


# Обновить данные таблиц
def update_table(coins_table, table_obj, table_update_obj, coins_list):
    session = db.connect_to_db()
    coins = set([c['coin_domain'] for c in coins_list])
    table_coins = set(db.get_table_coins(session, table_obj))
    coins_update = coins - table_coins
    db.clear_table(session, table_obj)
    db.write_all_to_table(session, table_obj, coins_list)
    db.clear_table(session, table_update_obj)
    coins_update = [db.get_coin_info(session, coins_table, c) for c in coins_update]
    db.write_all_to_table(session, table_update_obj, coins_update)


def get_all_coins(start_url, coins_table, table_obj, table_update_obj, promoted=False) -> list:
    coins_urls = get_all_coins_urls(start_url, promoted)  # Получение url адресов всех коинов категории
    coins = []  # Список коинов, который будет возвращен в результате работы функции
    new_coins = []  # Список новых коинов, которых не было в таблицах коинов в БД
    CONNECTIONS = 4  # Количество потоков сбора данных
    sess = db.connect_to_db()  # Подключение к базе данных
    table_coins_urls = db.get_coins_urls(sess, coins_table)  # Получение списка url адресов коинов из таблицы коинов сайта
    urls_for_parse = set(coins_urls) - set(table_coins_urls)  # Получение списка url адресов коинов, которых еще нет в БД
    urls_for_parse = list(urls_for_parse)
    #  Многопоточное выполнение парсинга страниц коинов
    with concurrent.futures.ThreadPoolExecutor(max_workers=CONNECTIONS) as executor:
        future_to_coin_url = (executor.submit(parse_coin_page, coin_url) for coin_url in urls_for_parse)
        for future in tqdm(concurrent.futures.as_completed(future_to_coin_url), total=len(urls_for_parse)):
            try:
                coin = future.result()
                if not db.check_coin(sess, coins_table, coin['coin_domain']):
                    db.write_to_coins_table(sess, coins_table, coin)  # Запись нового коина в таблицу коинов сайта БД
                    if not db.check_coin(sess, db.Coins, coin['coin_domain']):
                        db.write_to_Coins(sess, coin)  # Запись нового коина в общую таблицу коинов БД
                new_coins.append(coin)
            except Exception as ex:
                logging.exception(f'{ex}')
            finally:
                pass
    sess.commit()  # Применение к базе данных всех изменений
    #  Добавление информации о коине в итоговый список coins
    for coin_url in coins_urls:
        try:
            coin_website = db.get_coin_website_by_url(sess, coins_table, coin_url)  # Получение домена коина по его url адресу
        except Exception as ex:
            continue
        coin = db.get_coin_info(sess, coins_table, coin_website)  # Получение полной информации о коине по его домену
        coins.append(coin)
    #  Обновление таблиц table_obj и table_update_obj
    update_table(coins_table, table_obj, table_update_obj, coins)
    return coins


def update_gs_table(table_name, coins_table):
    sh = goosh.open_table_by_name(table_name)
    sheets = {'Promoted': db.CoinboomPromoted,
          'Promoted Update': db.CoinboomPromotedUpdate,
          'MarketCap toplist': db.CoinboomMarketcap,
          'MarketCap toplist Update': db.CoinboomMarketcapUpdate,
          'Daily Gainers': db.CoinboomDailyGainers,
          'Daily Gainers Update': db.CoinboomDailyGainersUpdate,
          'New Coins': db.CoinboomNewCoins,
          'New Coins Update': db.CoinboomNewCoinsUpdate,
          'Hot': db.CoinboomHot,
          'Hot Update': db.CoinboomHotUpdate
              }
    sess = db.connect_to_db()
    for ws_name, table_obj in sheets.items():
        coins = db.get_table_coins(sess, table_obj)
        coins_data = [db.get_coin_from_db(sess, coins_table, coin_domain) for coin_domain in coins]
        goosh.update_worksheet(sh, ws_name, coins_data)
        time.sleep(5)


def parse():
    get_all_coins(url_marketcap, db.CoinboomCoins, db.CoinboomMarketcap, db.CoinboomMarketcapUpdate)
    get_all_coins(url_daily_gainers, db.CoinboomCoins, db.CoinboomDailyGainers, db.CoinboomDailyGainersUpdate)
    get_all_coins(url_new_coins, db.CoinboomCoins, db.CoinboomNewCoins, db.CoinboomNewCoinsUpdate)
    get_all_coins(url_hot, db.CoinboomCoins, db.CoinboomHot, db.CoinboomHotUpdate)
    get_all_coins(url_promoted, db.CoinboomCoins, db.CoinboomPromoted, db.CoinboomPromotedUpdate,  promoted=True)
    update_gs_table('Coinboom.net', db.CoinboomCoins)


if __name__ == '__main__':
    parse()
    while True:
        if pycron.is_now('10 1 * * *'):  # каждый день в 01:10
            try:
                logging.info('Start parsing!')
                parse()
                logging.info('End parsing!')
            except Exception as ex:
                logging.critical(ex, ' - Global ERROR')
            time.sleep(60)
        else:
            # проверяем раз в 15 сек таймер
            time.sleep(15)
