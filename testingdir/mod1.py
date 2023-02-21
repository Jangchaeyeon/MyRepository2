# Databricks notebook source
#####################################################################################
# 생산실적(or 생산계획), 정대실적(or 정대계획) 순생산량 DB update
# exe_run_pipeline 과정 중 하나
# 1. update: 생산실적(or 생산계획), 정대실적(or 정대계획) 받아서 순생산량 계산 후 DB update
#####################################################################################
# import preprocessing_db.clean_product as clean_product
# import preprocessing_db.clean_line_stop as clean_line_stop
# import preprocessing_db.clean_plan as clean_plan
# import preprocessing_db.generate_15min_data as generate_15min_data
# import preprocessing_db.save_load_preprocessed_product as save_load_preprocessed_product

import configparser
import pandas as pd
import pyodbc
import datetime as datetime
import logging

# 1. update: 생산실적(or 생산계획), 정대실적(or 정대계획) 받아서 순생산량 계산 후 DB update
def update(site_name, # site_name ["daejeon1","daejeon2","jincheon"]
           today_date, # 처리날짜 20211201 실적은 D-7, 계획은 D-4 부터 업데이트 (datetime.datetime.today().strftime("%Y%m%d"))
           tb_type='actual'):# 'actual'(생산실적), 'plan'(생산계획)
    today_date = str(today_date)
    try :
        if tb_type == "actual":
            start_date_tmp = int(str((pd.to_datetime(today_date) - datetime.timedelta(days=7)).strftime("%Y%m%d")))
            end_date_tmp = int(today_date)
        else: # tb_type == "plan"
            #start_date_tmp = int(str((pd.to_datetime(today_date) - datetime.timedelta(days=4)).strftime("%Y%m%d")))
            #end_date_tmp =  int(str((pd.to_datetime(today_date) + datetime.timedelta(days=5)).strftime("%Y%m%d")))
            # 이영수 수정
            start_date_tmp = int(str((pd.to_datetime(today_date)).strftime("%Y%m")+"01"))
            end_date_tmp = int(str((pd.to_datetime(today_date) + datetime.timedelta(days=31)).strftime("%Y%m%d")))
        # end_date_tmp = int(today_date)

        # configparser로부터 DB 정보 가져오기
        conf = configparser.ConfigParser()
        conf.read('conf_db.ini', encoding='utf-8')

        # DB 정보
        db_ver = str(1)
        server = conf['db_info' + db_ver]['server']
        database = conf['db_info' + db_ver]['database']
        username = conf['db_info' + db_ver]['username']
        password = conf['db_info' + db_ver]['password']

        cnxn = pyodbc.connect(
            "DRIVER={SQL Server};SERVER=" + server + ";uid=" + username + ";pwd=" + password + ";DATABASE=" + database)
        cursor = cnxn.cursor()

        if tb_type == "actual":
            tb_name = conf['save_tb_name']['product_balanced']
            # --- 전처리
            product = clean_product.db_to_df(site_name, start_date_tmp, end_date_tmp)  # ---- DB로부터 데이터 받고 전처리
            line_stop = clean_line_stop.db_to_df(site_name, start_date_tmp, end_date_tmp)  # ---- DB로부터 데이터 받고 전처리
            line_stop = clean_line_stop.line_stop_adjust(line_stop)  # ---- DB로부터 데이터 받고 전처리
            product_balanced = generate_15min_data.balancing_product(product, line_stop)

        else: # tb_type == "plan"
            tb_name = conf['save_tb_name']['product_balanced_plan']
            # --- 전처리
            product, line_stop = clean_plan.db_to_df(site_name, start_date_tmp,  end_date_tmp)  # ---- DB로부터 데이터 받고 전처리
            product_balanced = generate_15min_data.balancing_product_plan(product, line_stop)  # ---- DB로부터 데이터 받고 전처리



        product_balanced['site_name'] = site_name
        product_balanced['datetime'] = pd.to_datetime(product_balanced['datetime'], errors='coerce')
        product_balanced['quantity'] = pd.to_numeric(product_balanced['quantity'], errors='coerce')

        # --- DB 삭제
        # ex. 2021-01-07일 실행 : 01~07일 데이터 업로드 -> 기존 업로드 되어있던 2021-01-01이후 데이터 삭제
        # check_sql = "SELECT MAX(DATETIME) FROM dbo."+ tb_name + " WHERE SITE_NAME = '" + site_name + "'"
        # max_date = pd.read_sql(check_sql, cnxn)
        # print("삭제 전 max_date : " , max_date)
        del_sql = ""
        plan_f_date = ""
        if tb_type == "actual":
            del_sql = "DELETE FROM dbo." + tb_name + " WHERE DATETIME >= '" + str(product_balanced["datetime"].min()) + "' AND  DATETIME<='" + str(product_balanced['datetime'].max())+ "' AND SITE_NAME = '" + site_name + "'"
        else:
            # --계획일 경우 전처리 테이블 삭제 시작일 계산 : 08시 이전 : today_date+1 / 08시 이후 : today_date+2 (내일 것은 클로징함)
            # --> 2022.10.21 이영수 수정              : 14시 이전 :  today_date+2 / 14시 이후 : today_date+3 (모래 것은 클로징함) [입찰용메일 발송 D-2 13시로 변경]
            if datetime.datetime.now().hour < 14:
                plan_f_date = (datetime.date.today() + datetime.timedelta(days=2)).strftime("%Y-%m-%d")
            else:
                plan_f_date = (datetime.date.today() + datetime.timedelta(days=3)).strftime("%Y-%m-%d")

            del_sql = "DELETE FROM dbo." + tb_name + " WHERE DATETIME >= '" + plan_f_date + "' AND  DATETIME<='" + str(product_balanced['datetime'].max()) + "' AND SITE_NAME = '" + site_name + "'"

        cursor.execute(del_sql)
        # check_sql = "SELECT MAX(DATETIME) FROM dbo."+ tb_name + " WHERE SITE_NAME = '" + site_name + "'"
        # max_date = pd.read_sql(check_sql, cnxn)
        # print("삭제 후 max_date : " , max_date)

        cnxn.commit()
        cursor.close()

        # --- DB 적재
        if tb_type == "actual":
            save_load_preprocessed_product.save(product_balanced, tb_type ='actual')
        else : # tb_type == "plan"
            save_load_preprocessed_product.save(product_balanced[product_balanced['datetime'] >= datetime.datetime.strptime(plan_f_date,"%Y-%m-%d")], tb_type ='plan')

        # check_sql = "SELECT MAX(DATETIME) FROM dbo." + tb_name + " WHERE SITE_NAME = '" + site_name + "'"
        # max_date = pd.read_sql(check_sql, cnxn)
        # print("적재 후 max_date : ", max_date)

        return print("(Success) update on DB : preprocessed product : ", tb_type)

    except Exception as e:
        logging.error(("\n" + "===========================================\n" +
                       "- ERROR    : [update_preprocessed_product.update]\n" +
                       "- DATE     : " + str(datetime.datetime.today()) + "\n" +
                       "- MESSAGE : \n"), exc_info=True)
        print("!!--------------- Error in [update_preprocessed_product.update] ---------------!!")
        print("- Date : ", datetime.datetime.today())
        print("- Exception: {}".format(type(e).__name__))
        print("- Exception message: {}".format(e))


# COMMAND ----------


