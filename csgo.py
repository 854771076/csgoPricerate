from os import system
from time import sleep
from requests import get
from logging import getLogger,StreamHandler,Formatter,INFO,DEBUG,ERROR, FileHandler
from datetime import datetime
from sys import stderr
from sqlalchemy import create_engine
import pymysql
from pandas import DataFrame,read_sql_table,merge
logger = getLogger(f'spider_log_{datetime.now()}')
logger.setLevel(INFO)
rf_handler = StreamHandler(stderr)#默认是sys.stderr
rf_handler.setLevel(DEBUG) 
rf_handler.setFormatter(Formatter("%(asctime)s - %(name)s - %(message)s"))
f_handler = FileHandler('error.log')
f_handler.setLevel(ERROR)
f_handler.setFormatter(Formatter("%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))
f2_handler = FileHandler(f'spider_log.log')
f2_handler.setLevel(INFO)
f2_handler.setFormatter(Formatter("%(asctime)s - %(levelname)s - %(filename)s[:%(lineno)d] - %(message)s"))
logger.addHandler(rf_handler)
logger.addHandler(f_handler)
logger.addHandler(f2_handler)
class MysqlDB():
    def __init__(self,user='csgo',password='csgo',host='106.14.66.39',port=3306,charset='utf8',db='csgo',**params):
        self.conn=create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}?charset={charset}')
    def save_pd(self,val,table:str,columns=None,method:str='replace'):
        '''
        method append,replace
        '''
        if columns!=None:
            df=DataFrame(val,columns=columns)
        else:
            df=DataFrame(val)
        df.to_sql(table,self.conn,if_exists=method,index=False)
        return df
    def read_pd(self,table:str):
        df=read_sql_table(con=self.conn,table_name=table)
        return df 
    def showtables(self):
        return self.conn.table_names()
    def getconn(self):
        return self.conn
def spider1():
    datas1=[]
    try:
        cookie=open('cookie_etopfun.txt','r',encoding='utf8').read()
    except:
        logger.error(f'找不到cookie_etopfun.txt')
        system('pause')
    headers={
        'Cookie':cookie,
        'Referer': 'https://www.etopfun.com/en/store/',
        'user-agent': r'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.41'
    }
    logger.info('开始爬取etopfun')
    try:
        r=get('https://www.etopfun.com/api/schema/bcitemlist.do?appid=730&mark_name=&rows=1000&page=1&quality=&rarity=&exterior=&lang=en',headers=headers)
    except Exception as e:
        logger.error(f'网络异常可能是没有开VPN----{e}')
        system('pause')
        return 
    page=r.json().get('datas','').get('pager','')
    if page=='':
        logger.error(f'网络异常可能是没有开VPN{r.json()}')
        return 0
    pages= page.get('pages','')
    sleep(3)
    for i in range(1,int(pages)+1):
        try:
            logger.info(f'开始爬取etopfun---{i}页---1000条/页---共{pages}页')
            r=get(f'https://www.etopfun.com/api/schema/bcitemlist.do?appid=730&mark_name=&rows=1000&page={i}&quality=&rarity=&exterior=&lang=en',headers=headers)
            data=r.json().get('datas').get('list')
            datas1+=data
            sleep(3)
        except Exception as e:
            logger.error(f'爬取失败---{i}页---80条/页---共{pages}页---{e}\n{r.text}')

            return
            
    logger.info('开始存储etopfun数据')
    df1=DataFrame(datas1).astype('str')
    df2=MysqlDB().save_pd(df1,'etopfun')
    logger.info('存储etopfun数据成功')
    return df2
def spider2():
    datas2=[]
    logger.info('开始爬取网易BUFF')
    try:
        cookie=open('cookie_buff.txt','r',encoding='utf8').read()
    except:
        logger.error(f'找不到cookie_buff.txt')
        system('pause')
    headers={
        'cookie':cookie,
        'user-agent': r'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.41'
    }
    r=get('https://buff.163.com/api/market/goods?game=csgo&page_num=1&use_suggestion=0&_=1679131111059&page_size=80',headers=headers)
    pager=r.json().get('data')
    pages=pager.get('total_page')
    sleep(3)
    for i in range(1,int(pages)+1):
        try:
            logger.info(f'开始爬取网易BUFF---{i}页---80条/页---共{pages}页')
            r=get(f'https://buff.163.com/api/market/goods?game=csgo&page_num={i}&use_suggestion=0&_=1679131111059&page_size=80',headers=headers)
            data=r.json().get('data').get('items')
            datas2+=data
            sleep(3)
        except Exception as e:
            logger.error(f'爬取失败---{i}页---{e}\n{r.text}')
            return
            
    logger.info('开始存储网易BUFF数据')
    buff=DataFrame(datas2).astype('str')
    df=MysqlDB().save_pd(buff,'buff')
    logger.info('存储网易BUFF数据成功')
    return df
def spider3():
    'https://www.etopfun.com/api/ingotitems/realitemback/orderlist.do?appid=730&page=1&rows=200&mark_name=&exterior=&quality=&rarity=&hero=&lang=en'
    datas3=[]
    try:
        cookie=open('cookie_etopfun.txt','r',encoding='utf8').read()
    except:
        logger.error(f'找不到cookie_etopfun.txt')
        system('pause')
    headers={
        'Cookie':cookie,
        'Referer': 'https://www.etopfun.com/en/store/',
        'user-agent': r'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36 Edg/111.0.1661.41'
    }
    logger.info('开始爬取etopfun_order')
    try:
        r=get('https://www.etopfun.com/api/ingotitems/realitemback/orderlist.do?appid=730&page=1&rows=200&mark_name=&exterior=&quality=&rarity=&hero=&lang=en',headers=headers)
    except Exception as e:
        logger.error(f'网络异常可能是没有开VPN----{e}')
        return 
    page=r.json().get('datas','').get('pager','')
    if page=='':
        logger.error(f'网络异常可能是没有开VPN{r.json()}')
        return 0
    pages= page.get('pages','')
    sleep(3)
    for i in range(1,int(pages)+1):
        try:
            logger.info(f'开始爬取etopfun_order---{i}页---200条/页---共{pages}页')
            r=get(f'https://www.etopfun.com/api/ingotitems/realitemback/orderlist.do?appid=730&page={i}&rows=200&mark_name=&exterior=&quality=&rarity=&hero=&lang=en',headers=headers)
            data=r.json().get('datas').get('list')
            datas3+=data
            sleep(3)
        except Exception as e:
            logger.error(f'爬取失败---{e}\n{r.text}')
            return
    logger.info('开始存储etopfun_order数据')
    df1=DataFrame(datas3).astype('str')
    df2=MysqlDB().save_pd(df1,'etopfun_order')
    logger.info('存储etopfun_order数据成功')
    return df2
def get_bilv():
    logger.info('开始读取etopfun数据库')
    df1=MysqlDB().read_pd('etopfun')
    logger.info('开始读取etopfun_order数据库')
    df1_order=MysqlDB().read_pd('etopfun_order')
    logger.info('开始读取buff数据库')
    df2=MysqlDB().read_pd('buff')
    logger.info('开始数据清洗')
    df3=merge(df1,df2,how='inner',left_on='name',right_on='market_hash_name')
    df3_order=merge(df1_order,df2,how='inner',left_on='name',right_on='market_hash_name')
    df3=df3.drop_duplicates()
    df3_order=df3_order.drop_duplicates()
    df3.value=df3.value.astype('float64')
    df3.sell_min_price=df3.sell_min_price.astype('float64')
    df3.quick_price=df3.quick_price.astype('float64')
    df3.sell_num=df3.sell_num.astype('float64')
    df3_order.value=df3_order.value.astype('float64')
    df3_order.sell_min_price=df3_order.sell_min_price.astype('float64')
    df3_order.quick_price=df3_order.quick_price.astype('float64')
    df3_order.sell_num=df3_order.sell_num.astype('float64')
    df3_order.num=df3_order.num.astype('float64')
    columns1=['英文名','中文名','简称','外网价格','buff最低价格','最新价格','buff在售数量','饰品类型','比率']
    columns2=['market_hash_name','name_y','short_name','value','sell_min_price','quick_price','sell_num','type_name','rate']
    columns1_order=['英文名','中文名','简称','外网价格','外网数量','buff最低价格','最新价格','buff在售数量','饰品类型','比率']
    columns2_order=['market_hash_name','name_y','short_name','value','num','sell_min_price','quick_price','sell_num','type_name','rate']
    df3['rate']=df3.sell_min_price/df3.value
    df3_order['rate']=df3_order.sell_min_price/df3_order.value
    df4=df3[columns2]
    df4_order=df3_order[columns2_order]
    df4.sort_values('rate',inplace=True)
    df4.columns=columns1
    df4_order.sort_values('rate',inplace=True)
    df4_order.columns=columns1_order
    logger.info('保存比率数据--比率数据.csv')
    logger.info('保存比率数据--比率数据_order')
    df4.to_csv(r'比率数据.csv',index=False)
    df4_order.to_csv(r'比率数据_order.csv',index=False)
    return True
def get_etopfun():
    logger.info('开始读取etopfun数据库')
    df2=MysqlDB().read_pd('etopfun')
    logger.info('保存etopfun原始数据---etopfun原始数据.csv')
    df2.to_csv(r'etopfun原始数据.csv',index=False)
    return df2
def get_etopfun_order():
    logger.info('开始读取etopfun_order数据库')
    df2=MysqlDB().read_pd('etopfun_order')
    logger.info('保存etopfun_order原始数据---etopfun_order原始数据.csv')
    df2.to_csv(r'etopfun_order原始数据.csv',index=False)
    return df2
def get_buff():
    logger.info('开始读取buff数据库')
    df1=MysqlDB().read_pd('buff')
    logger.info('保存buff原始数据---buff原始数据.csv')
    df1.to_csv(r'buff原始数据.csv',index=False)
    return df1
def menu():
    system('cls')
    print('========\t饰品爬虫系统\t=========')
    print('========\t1.爬取etopfun\t=========')
    print('========\t2.爬取buff\t=========')
    print('========\t3.爬取etopfun2\t=========')
    print('========\t4.导出原始数据\t=========')
    print('========\t5.导出分析数据\t=========')
    print('========\t6.一键执行\t=========')
    print('========\t0.退出系统\t=========')
def main():
    logger.info('进入系统')
    while True:
        menu()
        choice=input('请选择功能：')
        if choice=='1':
            spider1()
            logger.info(f'执行成功----{choice}')
            system('pause')
        elif choice=='2':
            spider2()
            logger.info(f'执行成功----{choice}')
            system('pause')
        elif choice=='4':
            try:
                get_etopfun()
                get_etopfun_order()
                get_buff()
            except Exception as e:
                logger.error(f'导出原始错误{e}')
            logger.info(f'执行成功----{choice}')
            system('pause')
        elif choice=='5':
            try:
                get_bilv()
            except Exception as e:
                logger.error(f'导出分析数据错误{e}')
            logger.info(f'执行成功----{choice}')
            system('pause')
        elif choice=='6':
            try:
                spider2()
                spider1()
                spider3()
                get_etopfun()
                get_buff()
                get_bilv()  
            except Exception as e:
                logger.error(f'错误：{e}')
            logger.info(f'执行成功----{choice}')
            system('pause')
        elif choice=='3':
            spider3()
            logger.info(f'执行成功----{choice}')
            system('pause')
        elif choice=='0':
            logger.info('退出系统')
            break
        else:
            pass
        
            
            
        
            
if __name__=='__main__':
    main()