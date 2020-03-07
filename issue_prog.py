# -*- coding: utf-8 -*-
import json
import traceback
import datetime
import requests
from multiprocessing.pool import ThreadPool as Pool
import cx_Oracle
from config.db_config import user, pwd, host, port, sid
from config.request_url import ec_url, grp_url, pmt_url, prc_url, empi_url
from config.table_name import KC21, KC22, KC24, N041, EC, GRP, PMT, ETL, PRC
import logging
import logging.handlers
import os
import time
import types
import copyreg
import sys
from multiprocessing import Manager


def _pickle_method(m):
    if m.im_self is None:
        return getattr, (m.im_class, m.im_func.func_name)
    else:
        return getattr, (m.im_self, m.im_func.func_name)


copyreg.pickle(types.MethodType, _pickle_method)


class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return (str(obj))
        else:
            return super().default(obj)


class MainControl:

    def __init__(self, process_count: int = 2):
        self.logger = logging.getLogger(__name__)
        log_name = os.path.join("log", "MainControl", "MainControl.log")
        hdl = logging.handlers.TimedRotatingFileHandler(filename=log_name, when="D", interval=1, backupCount=7,
                                                        encoding="utf-8")
        formatter = logging.Formatter('%(asctime)s %(module)s:%(lineno)d %(message)s')
        hdl.setFormatter(formatter)
        self.logger.addHandler(hdl)
        self.__process_count = process_count
        self.__local_db = cx_Oracle.connect(user=user, password=pwd, dsn="{}:{}/{}".format(host, port, sid),
                                            encoding="UTF-8", nencoding="UTF-8", threaded=True)
        self.__cursor = self.__local_db.cursor()
        self.__arraysize = 100

    def getting_start(self):
        sql = (
            "select * from {}"
        )

        # KC21 EMPI
        print("Start processing...")

        batch_rows = self.batch_rows(sql.format(KC21))
        while True:
            try:
                batch = next(batch_rows)
                m = Manager()
                share_ls = m.list()
                share_ls.extend(batch)
                del batch
                task = Pool(self.__process_count)
                for row in share_ls:
                    task.apply_async(self.empi, (row, "KC21"))
                task.close()
                task.join()
            except StopIteration:
                break

        batch_rows = self.batch_rows(sql.format(N041))

        while True:
            try:
                batch = next(batch_rows)
                m = Manager()
                share_ls = m.list()
                share_ls.extend(batch)
                del batch
                task = Pool(self.__process_count)
                for row in share_ls:
                    task.apply_async(self.process, (row, "N041"))
                task.close()
                task.join()
            except StopIteration:
                break

        # pairing check
        # N041
        batch_rows = self.batch_rows(sql.format(N041))

        while True:
            try:
                batch = next(batch_rows)
                m = Manager()
                share_ls = m.list()
                share_ls.extend(batch)
                del batch
                task = Pool(self.__process_count)
                for row in share_ls:
                    task.apply_async(self.pairing_check, (row, "N041"))
                task.close()
                task.join()
            except StopIteration:
                break

        # KC21
        batch_rows = self.batch_rows(sql.format(KC21))

        while True:
            try:
                batch = next(batch_rows)
                m = Manager()
                share_ls = m.list()
                share_ls.extend(batch)
                del batch
                task = Pool(self.__process_count)
                for row in share_ls:
                    task.apply_async(self.pairing_check, (row, "KC21"))
                task.close()
                task.join()
            except StopIteration:
                break

        # KC24
        batch_rows = self.batch_rows(sql.format(KC24))

        while True:
            try:
                batch = next(batch_rows)
                m = Manager()
                share_ls = m.list()
                share_ls.extend(batch)
                del batch
                task = Pool(self.__process_count)
                for row in share_ls:
                    task.apply_async(self.pairing_check, (row, "KC24"))
                task.close()
                task.join()
            except StopIteration:
                break

    def process(self, row: dict, table_name: str):
        # empi
        self.empi(row, table_name)

        # edit check
        if not row:
            return
        ec_req_data = {
            "data": row,
            "table_name": table_name
        }
        r = requests.post(ec_url, json=json.loads(json.dumps(ec_req_data, cls=DateTimeEncoder)))
        rj = r.json()
        print("quality control service result: {}".format(rj))
        if rj["status"] == "success":
            data = rj["result"]
            unnecessary = True
            for each in data:
                each["SBMT_TMSTMP"] = datetime.datetime.strptime(each["SBMT_TMSTMP"], "%Y-%m-%d %H:%M:%S")
                each["QRY_TMSTMP"] = datetime.datetime.strptime(each["QRY_TMSTMP"], "%Y-%m-%d %H:%M:%S.%f")
                each["BAH"] = row["BAH"]
                each["AKB021"] = row["USERNAME"]
                self.insert_or_update(EC, each, conn=self.__local_db)
                if each["QRY_LEVEL"] in [1, 2]:
                    unnecessary = False

            if unnecessary:
                self.grp_service(row)
            else:
                grp_result = {
                    "AAZ217": row["AAZ217"],
                    "SBMT_TMSTMP": row["SBMT_TMSTMP"],
                    "BAH": row["BAH"],
                    "AKB021": row["USERNAME"],
                    "RYSJ": row["RYSJ"],
                    "CYSJ": row["CYSJ"],
                    "MDC_CODE": "-",
                    "ADRG_CODE": "-",
                    "WEIGHT": 0.,
                    "DRG_CODE": "00",
                    "DRG_NAME": "not pass",
                    "GRP_VER": "edit check",
                    "EXCEPTION_TYPE": "-",
                    "GRP_TMSTMP": datetime.datetime.now()
                }
                self.insert_or_update(GRP, grp_result, conn=self.__local_db)
                # pmt
                self.pmt_service(grp_result)

        else:
            self.logger.error("data causing bug: {}".format(json.loads(json.dumps(ec_req_data, cls=DateTimeEncoder))))
            print("data causing bug: {}".format(json.loads(json.dumps(ec_req_data, cls=DateTimeEncoder))))

    def grp_service(self, row: dict):
        gre_request_data = {
            "AAZ217": row["AAZ217"],
            "BAH": row["BAH"],
            "SBMT_TMSTMP": row["SBMT_TMSTMP"],
            "jbdm": row["JBDM"],
            "xb": row["XB"] if row["XB"] else "",
            "nl": row["NL"] if row["NL"] else "",
            "sjzyts": row["SJZYTS"] if row["SJZYTS"] else "",
            "lyfs": row["LYFS"] if row["LYFS"] else "",
            "bzyzsnl": row["BZYZSNL"] if row["BZYZSNL"] else "",
            "xserytz": row["XSERYTZ"] if row["XSERYTZ"] else "",
            "zfy": row["ZFY"]
        }

        gr = requests.post(grp_url, data=json.loads(json.dumps(gre_request_data, cls=DateTimeEncoder)))
        grj = gr.json()
        print("group service result: {}".format(grj))
        if grj["is_success"] == "success":
            data = {
                "AAZ217": row["AAZ217"],
                "SBMT_TMSTMP": row["SBMT_TMSTMP"],
                "BAH": row["BAH"],
                "AKB021": row["USERNAME"],
                "RYSJ": row["RYSJ"],
                "CYSJ": row["CYSJ"],
                "MDC_CODE": "-",
                "ADRG_CODE": "-",
                "WEIGHT": 0.,
                "DRG_CODE": grj["DRGCODE"],
                "DRG_NAME": grj["DRGNAME"],
                "GRP_VER": grj["DRGVER"],
                "EXCEPTION_TYPE": "-",
                "GRP_TMSTMP": datetime.datetime.now()
            }
            self.insert_or_update(GRP, data, conn=self.__local_db)
            self.pmt_service(data)
        elif grj["msg"] == "exception":
            data = {
                "AAZ217": row["AAZ217"],
                "SBMT_TMSTMP": row["SBMT_TMSTMP"],
                "BAH": row["BAH"],
                "AKB021": row["USERNAME"],
                "RYSJ": row["RYSJ"],
                "CYSJ": row["CYSJ"],
                "MDC_CODE": "-",
                "ADRG_CODE": "-",
                "WEIGHT": 0.,
                "DRG_CODE": "-",
                "DRG_NAME": "-",
                "GRP_VER": "exception",
                "EXCEPTION_TYPE": "exception",
                "GRP_TMSTMP": datetime.datetime.now()
            }
        else:
            self.logger.error("group service error, data: {}, error message:{}".format(row, grj["message"]))
            print("group service error, data: {}, error message:{}".format(row, grj["message"]))

    def execute(self):
        yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
        today = datetime.datetime.now().strftime("%Y-%m-%d 00:00:00")
        tomorrow = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

        # self.etl()

        sql = (
            "select * from {} where SBMT_TMSTMP >= to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') "
            "and SBMT_TMSTMP < to_timestamp('{}', 'yyyy-mm-dd hh24:mi:ss') "
        )

        # KC21 empi
        batch_rows = self.batch_rows(sql.format(KC21, yesterday, today))
        while True:
            try:
                batch = next(batch_rows)
                m = Manager()
                share_ls = m.list()
                share_ls.extend(batch)
                del batch
                task = Pool(self.__process_count)
                for row in share_ls:
                    task.apply_async(self.empi, (row, "KC21"))
                task.close()
                task.join()
            except StopIteration:
                break

        batch_rows = self.batch_rows(sql.format(N041, yesterday, today))
        while True:
            try:
                batch = next(batch_rows)
                m = Manager()
                share_ls = m.list()
                share_ls.extend(batch)
                del batch
                task = Pool(self.__process_count)
                for row in share_ls:
                    task.apply_async(self.process, (row, "N041"))
                task.close()
                task.join()
            except StopIteration:
                break

        # pairing check
        # N041
        batch_rows = self.batch_rows(sql.format(N041, yesterday, today))

        while True:
            try:
                batch = next(batch_rows)
                m = Manager()
                share_ls = m.list()
                share_ls.extend(batch)
                del batch
                task = Pool(self.__process_count)
                for row in share_ls:
                    task.apply_async(self.pairing_check, (row, "N041"))
                task.close()
                task.join()
            except StopIteration:
                break

        # KC21
        batch_rows = self.batch_rows(sql.format(KC21, yesterday, today))

        while True:
            try:
                batch = next(batch_rows)
                m = Manager()
                share_ls = m.list()
                share_ls.extend(batch)
                del batch
                task = Pool(self.__process_count)
                for row in share_ls:
                    task.apply_async(self.pairing_check, (row, "KC21"))
                task.close()
                task.join()
            except StopIteration:
                break

        # KC24
        batch_rows = self.batch_rows(sql.format(KC24, yesterday, today))

        while True:
            try:
                batch = next(batch_rows)
                m = Manager()
                share_ls = m.list()
                share_ls.extend(batch)
                del batch
                task = Pool(self.__process_count)
                for row in share_ls:
                    task.apply_async(self.pairing_check, (row, "KC24"))
                task.close()
                task.join()
            except StopIteration:
                break

        sql = (
            "select * from {} where etl_date >= to_date('{}', 'yyyy-mm-dd hh24:mi:ss') "
            "and etl_date < to_date('{}', 'yyyy-mm-dd hh24:mi:ss')"
        ).format(ETL, today, tomorrow)

        row = self.row(sql)
        row["RUN"] = "True"
        self.insert_or_update(ETL, row, key="RUN")

    def etl(self):
        start_time = datetime.datetime.now()
        today = start_time.strftime("%Y-%m-%d 00:00:00")
        tomorrow = (datetime.datetime.now() + datetime.timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
        row = None
        print("Staring search update info from ETL_INFO...")
        while row is None:
            current_time = datetime.datetime.now()
            interval = (current_time - start_time).seconds / 3600
            if interval >= 24:
                self.logger.error("ETL_INFO no records after 24 hours")
                sys.exit()
            sql = (
                "select * from {} where etl_date >= to_date('{}', 'yyyy-mm-dd hh24:mi:ss') "
                "and etl_date < to_date('{}', 'yyyy-mm-dd hh24:mi:ss')"
            ).format(ETL, today, tomorrow)
            row = self.row(sql)
            if row is None:
                time.sleep(60)
            elif any(list(map(lambda x: row[x] == "False", ["KC21", "KC22", "KC24", "N041"]))):
                time.sleep(60)
            elif row["RUN"] == "True" or row["RUN"] == "In progress":
                self.logger.error("main control process has run already")
                sys.exit()
            elif any(list(map(lambda x: row[x] == "Fail", ["KC21", "KC22", "KC24", "N041"]))):
                for k, v in row.items():
                    if v == "Fail":
                        self.logger.error("etl procedure of {} failed".format(k))
                        print("etl procedure of {} failed".format(k))
        row["RUN"] = "In progress"
        self.insert_or_update(ETL, row, key="RUN")

    def pairing_check(self, row: dict, table_name: str):
        prc_req_data = {
            "source": table_name,
            "data": row
        }
        r = requests.post(prc_url, json=json.loads(json.dumps(prc_req_data, cls=DateTimeEncoder)))
        rj = r.json()
        print("pairing check result: {}".format(rj))
        if rj["status"] == "success":
            data = rj["result"]
            self.insert_or_update(PRC, data, self.__local_db)
        else:
            self.logger.error("pairing check service failed, data: {}".format(
                json.loads(json.dumps(prc_req_data, cls=DateTimeEncoder))))
            print("pairing check service failed, data: {}".format(
                json.loads(json.dumps(prc_req_data, cls=DateTimeEncoder))))

    def empi(self, row: dict, table_name: str):
        empi_req_data = {
            "source": table_name,
            "data": row
        }
        r = requests.post(empi_url, json=json.loads(json.dumps(empi_req_data, cls=DateTimeEncoder)))
        rj = r.json()
        print("empi result:{}".format(rj))
        if rj["status"] == "success":
            row["PAT_SN"] = rj["result"]["PAT_SN"]
            row["VIS_SN"] = rj["result"]["VIS_SN"]
            print(row)
            if table_name == "N041":

                self.insert_or_update(N041, row, main_key=["N041_INDEX"], conn=self.__local_db)
            else:
                self.insert_or_update(KC21, row, main_key=["KC21_INDEX"], conn=self.__local_db)
        else:
            self.logger.error(
                "empi service failed, data: {}".format(json.loads(json.dumps(empi_req_data, cls=DateTimeEncoder))))

    def pmt_service(self, row: dict):
        sql = (
            "select AKE029, AKC254, AKE035, AKC253 from ("
            "select AKE029, AKC254, AKE035, AKC253 from {} where AKC190 = '{}' order by SBMT_TMSTMP "
            ") where rownum = 1"
        ).format(KC24, row["AAZ217"])

        kc24_row = self.row(sql)
        info = {
            "AKE029": "",
            "AKC254": "",
            "AKE035": "",
            "AKC253": ""
        }
        if kc24_row is not None and len(kc24_row) != 0:
            info["AKE029"] = kc24_row["AKE029"] if kc24_row["AKE029"] else ""
            info["AKC254"] = kc24_row["AKC254"] if kc24_row["AKC254"] else ""
            info["AKE035"] = kc24_row["AKE035"] if kc24_row["AKE035"] else ""
            info["AKC253"] = kc24_row["AKC253"] if kc24_row["AKC253"] else ""

        pmt_request_data = {
            "AAZ217": row["AAZ217"],
            "SBMT_TMSTMP": row["SBMT_TMSTMP"],
            "BAH": row["BAH"],
            "YBJSDJ": 3,
            "YBLX": 1,
            "DRG_CODE": row["DRG_CODE"],
            "AKE029": info["AKE029"],
            "AKC254": info["AKC254"],
            "AKE035": info["AKE035"],
            "AKC253": info["AKC253"]
        }
        pmt = requests.post(pmt_url, data=json.loads(json.dumps(pmt_request_data, cls=DateTimeEncoder)))
        pmtj = pmt.json()
        print("payment service result: {}".format(pmtj))
        if pmtj["is_success"] == "success":
            data = {
                "AAZ217": row["AAZ217"],
                "SBMT_TMSTMP": row["SBMT_TMSTMP"],
                "BAH": row["BAH"],
                "AKB021": row["AKB021"],
                "YBJSDJ": 3,
                "YBLX": 1,
                "RYSJ": row["RYSJ"],
                "CYSJ": row["CYSJ"],
                "AKE029": info["AKE029"],
                "AKC253": info["AKC253"],
                "AKE039": pmtj["AKE039"],
                "AKC254": info["AKC254"],
                "AKE035": info["AKE035"],
                "AKC264": pmtj["AKC264"],
                "DRG_CODE": row["DRG_CODE"],
                "FEE_BY_MEDICARE": pmtj["fee_by_medicare"],
                "FEE_BY_PERSONAL": pmtj["fee_by_personal"],
                "PMT_VER": pmtj["PMT_VER"],
                "PMT_TMSTMP": datetime.datetime.now()
            }
            self.insert_or_update(PMT, data, conn=self.__local_db)
        elif pmtj["msg"] == "DRG_CODE不在支付标准中":
            data = {
                "AAZ217": row["AAZ217"],
                "SBMT_TMSTMP": row["SBMT_TMSTMP"],
                "BAH": row["BAH"],
                "AKB021": row["AKB021"],
                "YBJSDJ": 3,
                "YBLX": 1,
                "RYSJ": row["RYSJ"],
                "CYSJ": row["CYSJ"],
                "AKE029": info["AKE029"],
                "AKC253": info["AKC253"],
                "AKE039": 0,
                "AKC254": info["AKC254"],
                "AKE035": info["AKE035"],
                "AKC264": 0,
                "DRG_CODE": row["DRG_CODE"],
                "FEE_BY_MEDICARE": 0,
                "FEE_BY_PERSONAL": 0,
                "PMT_VER": "0",
                "PMT_CAT": pmtj["msg"],
                "PMT_TMSTMP": datetime.datetime.now()
            }
            self.insert_or_update(PMT, data, conn=self.__local_db)
        else:
            self.logger.error("payment service error, data: {}, "
                         "error message: {}".format(pmt_request_data, pmtj["msg"]))
            print("payment service error, data: {}, error message: {}".format(pmt_request_data, pmtj["msg"]))

    def row(self, sql: str):
        with self.__local_db.cursor() as cursor:
            try:
                cursor.execute(sql)
            except cx_Oracle.DatabaseError:
                self.logger.error(traceback.format_exc(), "sql: {}".format(sql))
                print(traceback.format_exc())
                print("sql: {}".format(sql))
                return
            result = cursor.fetchone()
            if result:
                fields = cursor.description
                data_dict = dict(zip((field[0] for field in fields), result))
                return data_dict
            else:
                return

    def batch_rows(self, sql: str):
        with self.__local_db.cursor() as cursor:
            cursor.arraysize = self.__arraysize
            try:
                cursor.execute(sql)
            except cx_Oracle.DatabaseError:
                self.logger.error(traceback.format_exc(), "sql: {}".format(sql))
                print(traceback.format_exc())
                print("sql: {}".format(sql))
                return
            fields = cursor.description
            result = cursor.fetchmany()
            while len(result) != 0:
                for i in range(len(result)):
                    result[i] = dict(zip((field[0] for field in fields), result[i]))
                yield result
                result = cursor.fetchmany()

    def insert_or_update(self, table_name: str, data: dict, conn: cx_Oracle.Connection = None, key: str = None,
                         main_key: [str] = None,):
        if conn:
            cursor = conn.cursor()
        else:
            cursor = self.__cursor
        field = ','.join(data.keys())
        param = [':' + str(e) for e in data.keys()]
        sql = (
            "insert into {} ({}) "
            "values ({}) "
        ).format(table_name, field, ','.join(param))
        value = [e for e in data.values()]
        try:
            cursor.execute(sql, value)
        except cx_Oracle.IntegrityError:
            upd_sql = (
                "UPDATE {} "
            ).format(table_name)
            set_statement = "set " + ", ".join(["{} = :{}".format(k, k) for k in data.keys()])
            if main_key is None:
                where_statement = " where " + " and ".join(["{} = :{}".format(k, k) for k in data.keys() if k != key])
            else:
                where_statement = " where " + " and ".join(["{} = :{}".format(k, k) for k in main_key])
            upd_sql += set_statement + where_statement
            cursor.execute(upd_sql, data)
        except cx_Oracle.DatabaseError:
            self.logger.error(traceback.format_exc(), "sql: {}".format(sql))
            print(traceback.format_exc())
            print("sql: {}".format(sql))
        if conn:
            conn.commit()
        else:
            self.__local_db.commit()


if __name__ == "__main__":

    mc = MainControl()
    mc.execute()

