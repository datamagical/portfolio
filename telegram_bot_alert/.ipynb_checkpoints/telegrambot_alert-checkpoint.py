# импортируем необходимые библиотеки для скрипта
from datetime import datetime, timedelta
import pandas as pd
from io import StringIO
import requests
import pandahouse as ph
import telegram
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import plotly.express as px
import io

# библиотеки для работы с airflow
from airflow.decorators import dag, task
from airflow.operators.python import get_current_context


# Функция для ClickHouse, которая принимает на вход словарик для доступа к базе данных
connection = {'host': 'название хоста',
                      'database':'название необходиой базы данных',
                      'user':'название пользователя', 
                      'password':'пароль пользователя'
                     }

#данные для телеграм бота
my_token = 'токен'
group_chat_id = 'id чата, куда будут приходить сообщения от бота'


# параметры, которые передаём в таски
default_args = {
    'owner': '****', # имя владельца операции
    'depends_on_past': False, # зависит ли от прошлых запусков
    'retries': 2, # количество попыток для рестарта дага
    'retry_delay': timedelta(minutes=5), # промежуток для рестарта
    'start_date': datetime(2024, 2, 15) # дата начала выполнения дага
}

# Интервал запуска DAG
schedule_interval = '*/15 * * * *' # в данном случае запускается каждые 15 минут
  
# напишем функцию для выявление алертов
def alert_anomaly(df, metric, a=3, n=5): # на вход функции подаём данные, рассматриваемую метрику, а также постоянные значения, коэффициент a, определяющий ширину интервала, n - величина периода
    # задаём 25 квантиль и 75 квантиль
    df['Q25'] = df[metric].shift(1).rolling(n).quantile(0.25) 
    df['Q75'] = df[metric].shift(1).rolling(n).quantile(0.75)
    df['iqr'] = df['Q75'] - df['Q25'] # IQR величина межквартильного размаха
    df['up'] = df['Q75'] + a*df['iqr'] # формула для верхней границы
    df['low'] = df['Q25'] - a*df['iqr'] # формула для нижней границы
    # сгладим показатели верхней и нижней границы, для более ровного (не рванного) графика
    df['up'] = df['up'].rolling(n, center=True).mean()
    df['low'] = df['low'].rolling(n, center=True).mean()

    # зададим условие, в котором при успешном выполнении значению is_alert присваивается 1
    if df[metric].iloc[-1] < df['low'].iloc[-3] or df[metric].iloc[-1] > df['up'].iloc[-3]:
        is_alert = 1
    else:
        is_alert = 0

    return is_alert, df

def run_alert(chat_id):
    my_token = 'токен'
    bot = telegram.Bot(token=my_token)
    group_chat_id = 'id чата, куда будут приходить сообщения от бота'
    # запрос к базе данных, возвращающий метрики likes, views, uniq_users, uniq_msg, CTR, за каждые 15 минут
    q ="""
    select *
    from
    (
    select 
        toStartOfInterval(time, INTERVAL 15 minute) as day_event,
        uniqExact(user_id) as uniq_users,
        countIf(action='like') as likes,
        countIf(action='view') as views,
        round(likes / views, 3) as CTR
    from simulator_20240120.feed_actions
    where day_event >= today() - 1 and day_event < toStartOfInterval(now(), INTERVAL 15 minute)
    group by day_event
    order by day_event) t1

    join

    (
    select 
        toStartOfInterval(time, INTERVAL 15 minute) as day_event,
        uniqExact(user_id) as uniq_users_msg,
        uniqExact(receiver_id) as uniq_msg
    from simulator_20240120.message_actions
    where day_event >= today() - 1 and day_event < toStartOfInterval(now(), INTERVAL 15 minute)
    group by day_event
    order by day_event) t2

    using day_event
        """
    data = ph.read_clickhouse(q, connection=connection)
    # алерты будем присылать по следующим метрикам
    metrics_list = ['likes', 'views', 'CTR', 'uniq_users_msg', 'uniq_msg']
    # при помощи цикла будем сравнивать на каждом этапе определенную метрику, с такой же метрикой с разницей в 45 минут
    for metric in metrics_list:
        df = data[['day_event', metric]].copy()

        is_alert, df = alert_anomaly(df, metric)

        if is_alert == 1:
        # в случае is_alert = 1, будем присылать следующее сообщение в телеграм чат        
            msg = '''Метрика {metric} дата: {current_date} время:{current_time}\nтекущее значение {current:.2f}\nотклонение от предыдущего значения {last_val:.2%}'''.format(metric=metric,
                                                                                                                          current_date=df['day_event'].iloc[-1].strftime('%Y-%m-%d'),
                                                                                                                          current_time=df['day_event'].iloc[-1].strftime('%H:%M'),
                                                                                                                          current=df[metric].iloc[-1],
                                                                                                                          last_val=abs(1-(df[metric].iloc[-1]/df[metric].iloc[-2])))
            # параметры для графиков по каждой метрике
            sns.set(rc={'figure.figsize':(16,12)}, style="whitegrid")
            ax = sns.lineplot(x=df['day_event'], y=df[metric], label='metric')
            ax = sns.lineplot(x=df['day_event'], y=df['up'], label='верхний предел')
            ax = sns.lineplot(x=df['day_event'], y=df['low'], label='нижний предел')
            plot_object = io.BytesIO()
            plt.savefig(plot_object)
            plot_object.seek(0)
            plot_object.name = 'test_plot.png'
            plt.close()
            bot.sendMessage(chat_id=group_chat_id, text=msg)
            bot.sendPhoto(chat_id=group_chat_id, photo=plot_object)
    return
# создадим даг с именем dag_alert_bot_ev, определим функции рассмотренные выше       
@dag(default_args=default_args, schedule_interval=schedule_interval, catchup=False)
def dag_alert_bot_ev():             
    @task()
    def report_alert():
        run_alert(chat_id=group_chat_id)
    report_alert()

my_alert_tgbot = dag_alert_bot_ev()
