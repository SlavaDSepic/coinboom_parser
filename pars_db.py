from sqlalchemy import create_engine, MetaData, Table, Integer, String, \
    Column, DateTime, ForeignKey, Numeric
from sqlalchemy.ext.declarative import declarative_base
from datetime import datetime
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from config import DB_NAME, DB_ADDR, DB_PASS, DB_USER


engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_ADDR}/{DB_NAME}", pool_size=20, max_overflow=30)
engine.connect()
Session = sessionmaker(bind=engine)
Base = declarative_base()


class Coins(Base):
    __tablename__ = 'coins'
    id = Column(Integer, primary_key=True)
    coin_name = Column(String(250), nullable=False)
    coin_short_name = Column(String(250), nullable=False)
    coin_domain = Column(String(1000), nullable=False, unique=True)
    coin_domains_other = Column(String(1000), nullable=False)
    telegram = Column(String(1000))
    twitter = Column(String(1000))
    facebook = Column(String(1000))
    discord = Column(String(1000))
    reddit = Column(String(1000))
    linkedin = Column(String(1000))
    bitcointalk = Column(String(1000))
    medium = Column(String(1000))
    instagram = Column(String(1000))
    youtube = Column(String(1000))
    tiktok = Column(String(1000))
    other_social_links = Column(String(10000))
    coin_description = Column(String(30000))
    coin_audit = Column(String(1000))
    coin_listing_status = Column(String(250))
    coin_launch = Column(String(200), nullable=False)
    coin_presale_status = Column(String(250))


class CoinboomCoins(Base):
    __tablename__ = 'coinboom_coins'
    id = Column(Integer, primary_key=True)
    coin_name = Column(String(250), nullable=False)
    coin_short_name = Column(String(250), nullable=False)
    coin_url = Column(String(1000), nullable=False)
    coin_domain = Column(String(1000), nullable=False, unique=True)
    coin_domains_other = Column(String(1000), nullable=False)
    telegram = Column(String(1000))
    twitter = Column(String(1000))
    facebook = Column(String(1000))
    discord = Column(String(1000))
    reddit = Column(String(1000))
    linkedin = Column(String(1000))
    bitcointalk = Column(String(1000))
    medium = Column(String(1000))
    instagram = Column(String(1000))
    youtube = Column(String(1000))
    tiktok = Column(String(1000))
    other_social_links = Column(String(10000))
    coin_description = Column(String(30000))
    coin_audit = Column(String(1000))
    coin_listing_status = Column(String(250))
    coin_launch = Column(String(200), nullable=False)
    coin_presale_status = Column(String(250))


class CoinboomMarketcap(Base):
    __tablename__ = 'coinboom_marketcap'
    id = Column(Integer, primary_key=True)
    coin_name = Column(String(250), nullable=False)
    coin_short_name = Column(String(250), nullable=False)
    coin_domain = Column(String(1000), nullable=False)
    coin_info_datetime = Column(DateTime(), default=datetime.now)


class CoinboomMarketcapUpdate(Base):
    __tablename__ = 'coinboom_marketcap_update'
    id = Column(Integer, primary_key=True)
    coin_name = Column(String(250), nullable=False)
    coin_short_name = Column(String(250), nullable=False)
    coin_domain = Column(String(1000), nullable=False)
    coin_info_datetime = Column(DateTime(), default=datetime.now)


class CoinboomDailyGainers(Base):
    __tablename__ = 'coinboom_daily_gainers'
    id = Column(Integer, primary_key=True)
    coin_name = Column(String(250), nullable=False)
    coin_short_name = Column(String(250), nullable=False)
    coin_domain = Column(String(1000), nullable=False)
    coin_info_datetime = Column(DateTime(), default=datetime.now)


class CoinboomDailyGainersUpdate(Base):
    __tablename__ = 'coinboom_daily_gainers_update'
    id = Column(Integer, primary_key=True)
    coin_name = Column(String(250), nullable=False)
    coin_short_name = Column(String(250), nullable=False)
    coin_domain = Column(String(1000), nullable=False)
    coin_info_datetime = Column(DateTime(), default=datetime.now)


class CoinboomNewCoins(Base):
    __tablename__ = 'coinboom_new_coins'
    id = Column(Integer, primary_key=True)
    coin_name = Column(String(250), nullable=False)
    coin_short_name = Column(String(250), nullable=False)
    coin_domain = Column(String(1000), nullable=False)
    coin_info_datetime = Column(DateTime(), default=datetime.now)


class CoinboomNewCoinsUpdate(Base):
    __tablename__ = 'coinboom_new_coins_update'
    id = Column(Integer, primary_key=True)
    coin_name = Column(String(250), nullable=False)
    coin_short_name = Column(String(250), nullable=False)
    coin_domain = Column(String(1000), nullable=False)
    coin_info_datetime = Column(DateTime(), default=datetime.now)


class CoinboomHot(Base):
    __tablename__ = 'coinboom_hot'
    id = Column(Integer, primary_key=True)
    coin_name = Column(String(250), nullable=False)
    coin_short_name = Column(String(250), nullable=False)
    coin_domain = Column(String(1000), nullable=False)
    coin_info_datetime = Column(DateTime(), default=datetime.now)


class CoinboomHotUpdate(Base):
    __tablename__ = 'coinboom_hot_update'
    id = Column(Integer, primary_key=True)
    coin_name = Column(String(250), nullable=False)
    coin_short_name = Column(String(250), nullable=False)
    coin_domain = Column(String(1000), nullable=False)
    coin_info_datetime = Column(DateTime(), default=datetime.now)


class CoinboomPromoted(Base):
    __tablename__ = 'coinboom_promoted'
    id = Column(Integer, primary_key=True)
    coin_name = Column(String(250), nullable=False)
    coin_short_name = Column(String(250), nullable=False)
    coin_domain = Column(String(1000), nullable=False)
    coin_info_datetime = Column(DateTime(), default=datetime.now)


class CoinboomPromotedUpdate(Base):
    __tablename__ = 'coinboom_promoted_update'
    id = Column(Integer, primary_key=True)
    coin_name = Column(String(250), nullable=False)
    coin_short_name = Column(String(250), nullable=False)
    coin_domain = Column(String(1000), nullable=False)
    coin_info_datetime = Column(DateTime(), default=datetime.now)


if __name__ == '__main__':
    # ?????????????? ??????????????
    Base.metadata.create_all(engine)

    # ?????????????? ?????? ??????????????
    # Base.metadata.drop_all(engine)


#  ???????????????????? ???????????????????? ?? ????, ???????????????????? ???????????? ????????????
def connect_to_db():
    session = Session()
    return session


#  ???????????????? ?????? ???????????????????? ???? ?????????? ?????????? ?? ?????????? ?????????????? coins
def write_to_Coins(session, coin_info):
    coin = Coins(
        coin_name=coin_info['coin_name'],
        coin_short_name=coin_info['coin_short_name'],
        coin_domain=coin_info['coin_domain'],
        coin_domains_other=coin_info['coin_domains_other'],
        telegram=coin_info['telegram'],
        twitter=coin_info['twitter'],
        facebook=coin_info['facebook'],
        discord=coin_info['discord'],
        reddit=coin_info['reddit'],
        linkedin=coin_info['linkedin'],
        bitcointalk=coin_info['bitcointalk'],
        medium=coin_info['medium'],
        instagram=coin_info['instagram'],
        youtube=coin_info['youtube'],
        tiktok=coin_info['tiktok'],
        other_social_links=coin_info['other_social_links'],
        coin_description=coin_info['coin_description'],
        coin_audit=coin_info['coin_audit'],
        coin_listing_status=coin_info['coin_listing_status'],
        coin_launch=coin_info['coin_launch'],
        coin_presale_status=coin_info['coin_presale_status']
    )
    try:
        session.add(coin)
        # session.commit()
    except Exception as ex:
        session.connect()
        print(coin_info['coin_name'], ' write_to_Coins ERROR')
        raise ex


#  ???????????????? ?????? ???????????????????? ???? ?????????? ?????????? ?? ?????????????? coins_table
def write_to_coins_table(session, coins_table, coin_info):
    coin = coins_table(
        coin_name=coin_info['coin_name'],
        coin_short_name=coin_info['coin_short_name'],
        coin_url=coin_info['coin_url'],
        coin_domain=coin_info['coin_domain'],
        coin_domains_other=coin_info['coin_domains_other'],
        telegram=coin_info['telegram'],
        twitter=coin_info['twitter'],
        facebook=coin_info['facebook'],
        discord=coin_info['discord'],
        reddit=coin_info['reddit'],
        linkedin=coin_info['linkedin'],
        bitcointalk=coin_info['bitcointalk'],
        medium=coin_info['medium'],
        instagram=coin_info['instagram'],
        youtube=coin_info['youtube'],
        tiktok=coin_info['tiktok'],
        other_social_links=coin_info['other_social_links'],
        coin_description=coin_info['coin_description'],
        coin_audit=coin_info['coin_audit'],
        coin_listing_status=coin_info['coin_listing_status'],
        coin_launch=coin_info['coin_launch'],
        coin_presale_status=coin_info['coin_presale_status']
    )
    try:
        session.add(coin)
        # session.commit()
    except Exception as ex:
        print(coin_info['coin_url'], ' write_to_coins_table ERROR')
        raise ex


#  ???????????????? ???????????????? ???????????????????? ???? ?????????? ?????????? ?? ?????????????? table_obj
def write_to_table(session, table_obj, coin_info):
    if not coin_info:
        return None
    coin = table_obj(
        coin_name=coin_info['coin_name'],
        coin_short_name=coin_info['coin_short_name'],
        coin_domain=coin_info['coin_domain']
    )
    try:
        session.add(coin)
        session.commit()
    except Exception as ex:
        print(f'Coin {coin_info["coin_url"]} write_to_table ERROR')
        raise ex


#  ???????????????? ???????????????? ???????????????????? ?? ???????? ???????????? ?? ?????????????? table_obj
def write_all_to_table(session, table_obj, coins_list):
    coins = []
    for coin_info in coins_list:
        coin = table_obj(
            coin_name=coin_info['coin_name'],
            coin_short_name=coin_info['coin_short_name'],
            coin_domain=coin_info['coin_domain']
        )
        coins.append(coin)
    try:
        session.add_all(coins)
        session.commit()
    except Exception as ex:
        print(len(coins), ' coins -- write_all_to_table ERROR')
        raise ex


#  ?????????????????? ???????? ???? ???????? ?? ?????????????? ???? ?????? ????????????
def check_coin(session, table_obj, coin_domain):
    try:
        session.query(table_obj).filter_by(coin_domain=coin_domain).one()
        return True
    except Exception:
        return False


#  ???????????????????? ???????????? ?????????????? ????????????, ?????????????? ???????? ?? ?????????????? table_obj
def get_table_coins(session, table_obj):
    query = [x[0] for x in session.query(table_obj.coin_domain).all()]
    return query


#  ???????????????????? ?????????????? ?? ?????????????????????? ?? ?????????? ?? ?????????????? ???????????? ??????????
def get_coin_info(session, coins_table, coin_domain):
    coin = session.query(coins_table).filter(coins_table.coin_domain == coin_domain).one()
    return {'coin_name': coin.coin_name,
            'coin_short_name': coin.coin_short_name,
            'coin_url': coin.coin_url,
            'coin_domain': coin.coin_domain,
            'coin_domains_other': coin.coin_domains_other,
            'telegram': coin.telegram,
            'twitter': coin.twitter,
            'facebook': coin.facebook,
            'discord': coin.discord,
            'reddit': coin.reddit,
            'linkedin': coin.linkedin,
            'bitcointalk': coin.bitcointalk,
            'medium': coin.medium,
            'instagram': coin.instagram,
            'youtube': coin.youtube,
            'tiktok': coin.tiktok,
            'other_social_links': coin.other_social_links,
            'coin_description': coin.coin_description,
            'coin_audit': coin.coin_audit,
            'coin_listing_status': coin.coin_listing_status,
            'coin_launch': coin.coin_launch,
            'coin_presale_status': coin.coin_presale_status
    }


def get_coin_from_db(session, coins_table, coin_domain):
    coin = session.query(coins_table).filter(coins_table.coin_domain == coin_domain).one()
    return [coin.coin_name,
            coin.coin_short_name,
            coin.coin_url,
            coin.coin_domain,
            coin.coin_domains_other,
            coin.telegram,
            coin.twitter,
            coin.facebook,
            coin.discord,
            coin.reddit,
            coin.linkedin,
            coin.bitcointalk,
            coin.medium,
            coin.instagram,
            coin.youtube,
            coin.tiktok,
            coin.other_social_links,
            coin.coin_description,
            coin.coin_audit,
            coin.coin_listing_status,
            coin.coin_launch,
            coin.coin_presale_status
    ]


#  ???????????????????? ?????????????? ?? ?????????????????????? ?? ?????????? ?? ?????????? ?????????????? ????????????
def get_global_coin_info(session, coin_domain):
    try:
        coin = session.query(Coins).filter(Coins.coin_domain == coin_domain).one()
    except Exception as ex:
        print(coin_domain, 'get_global_coin_info ERROR')
        raise ex
    return {'coin_name': coin.coin_name,
            'coin_short_name': coin.coin_short_name,
            'coin_domain': coin.coin_domain,
            'coin_domains_other': coin.coin_domains_other,
            'telegram': coin.telegram,
            'twitter': coin.twitter,
            'facebook': coin.facebook,
            'discord': coin.discord,
            'reddit': coin.reddit,
            'linkedin': coin.linkedin,
            'bitcointalk': coin.bitcointalk,
            'medium': coin.medium,
            'instagram': coin.instagram,
            'youtube': coin.youtube,
            'tiktok': coin.tiktok,
            'other_social_links': coin.other_social_links,
            'coin_description': coin.coin_description,
            'coin_audit': coin.coin_audit,
            'coin_listing_status': coin.coin_listing_status,
            'coin_launch': coin.coin_launch,
            'coin_presale_status': coin.coin_presale_status
    }


#  ???????????????????? ???????????? url ?????????????? ???????????? ?? ?????????????? ???????????? ??????????
def get_coins_urls(session, coins_table):
    query = [x[0] for x in session.query(coins_table.coin_url).all()]
    return query


#  ???????????????????? ?????????? ?????????? ???? ?????? url ???????????? ???? ?????????????? ???????????? ??????????
def get_coin_website_by_url(session, coins_table, coin_url):
    query = session.query(coins_table.coin_domain).filter(coins_table.coin_url == coin_url).one()[0]
    return query


#  ?????????????? ?????? ???????????? ?????????????? table_obj
def clear_table(session, table_obj):
    session.query(table_obj).delete(synchronize_session="fetch")
    session.commit()
