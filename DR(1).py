import multiprocessing as mp
import os
import time

import cx_Oracle  # 引用模块cx_Oracle
import numpy as np
import xlwt

os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'



def FIND_PK(module_pre):
    conn = cx_Oracle.connect('c##LKX/0000@219.216.69.63:1521/orcl')  # 连接数据库
    cur = conn.cursor()
    table_dict = {}  # 表结构字典
    # table_dict = {
    #     table_name:{
    #         'cols':{
    #              'c1':列类型，
    #         }，
    #         'primary_key':[主键列表]
    #     }
    # }
    # 获取表名列表并去除空表
    if module_pre == 'all':
        cur.execute(
                "SELECT TABLE_NAME from all_tables where  OWNER='C##SCYW'")
    else:
        cur.execute(
                "SELECT TABLE_NAME from all_tables where TABLE_NAME like '" + module_pre + "%' and OWNER='C##SCYW'")
    res = list(np.array(cur.fetchall())[:, 0])
    table_list = {}
    # table_list = {
    #     table:{
    #         'cols':{
    #              'c1':列类型，
    #         }，
    #         'primary_key':[主键列表]
    #     }
    # }
    # 获取表自详细信息结构 存入table_list
    if module_pre == 'all':
        cur.execute(
                "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE from  all_tab_cols where  OWNER='C##SCYW'")  # 用cursor进行各种操作
    else:
        cur.execute(
                "SELECT TABLE_NAME,COLUMN_NAME,DATA_TYPE from  all_tab_cols where TABLE_NAME like '" + module_pre + "%' and OWNER='C##SCYW'")
    results = cur.fetchall()
    for result in results:
        if result[0] in res:
            if result[0] in table_list:
                table_list[result[0]]['cols'][result[1]] = result[2]
            else:
                table_list[result[0]] = {}
                table_list[result[0]]['cols'] = {}
                table_list[result[0]]['primary_key'] = []
                table_list[result[0]]['cols'][result[1]] = result[2]
                
    # 根据table_list来判断主键
    # 获取已定义主键信息
    if module_pre == 'all':
        cur.execute(
                "select CC.COLUMN_NAME,C.TABLE_NAME from all_constraints c,ALL_CONS_COLUMNS cc where C.OWNER = CC.OWNER AND C.OWNER = 'C##SCYW' AND C.CONSTRAINT_TYPE = 'P' AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME   AND C.TABLE_NAME = CC.TABLE_NAME")
    else:
        cur.execute(
                "select CC.COLUMN_NAME,C.TABLE_NAME from all_constraints c,ALL_CONS_COLUMNS cc where C.OWNER = CC.OWNER AND C.OWNER = 'C##SCYW'   AND C.TABLE_NAME like '" + module_pre + "%'   AND C.CONSTRAINT_TYPE = 'P'   AND C.CONSTRAINT_NAME = CC.CONSTRAINT_NAME   AND C.TABLE_NAME = CC.TABLE_NAME")
    for result in cur:
        if result[1] in table_list:
            if 'primary_key' in table_list[result[1]]:
                table_list[result[1]]['primary_key'].append(result[0])
            else:
                table_list[result[1]]['primary_key'] = []
                table_list[result[1]]['primary_key'].append(result[0])
    # 对于没有主键的表进行主键判断
    for table in table_list:
        if len(table_list[table]['primary_key']) == 0:
            for col_name in list(table_list[table]['cols']):
                cur.execute(
                        "SELECT " + col_name + ",  COUNT(" + col_name + ")FROM c##SCYW." + table + " GROUP BY " + col_name + " HAVING  COUNT(" + col_name + ") > 1")
                duplicate_count = len(cur.fetchall())
                if (duplicate_count == 0):
                    cur.execute("SELECT COUNT("+col_name+")FROM c##SCYW."+table+" where "+col_name+" is not null")
                    if(cur.fetchone()[0] > 0):
                        if table_list[table]['cols'][col_name] != 'NUMBER':
                            table_list[table]['primary_key'].append(col_name)
                        else:
                            cur.execute(
                                    "select DATA_SCALE from all_tab_cols WHERE TABLE_NAME='" + table + "' and COLUMN_NAME = '" + col_name + "' and OWNER = 'C##SCYW'")
                            # print('scale:', cur.fetchone()[0])
                            if (cur.fetchone()[0] == 0):
                                table_list[table]['primary_key'].append(col_name)
    return table_list

# 判断外键函数
def Duplicate_Rate(table_list):
    conn = cx_Oracle.connect('c##LKX/0000@219.216.69.63:1521/orcl')  # 连接数据库
    cur = conn.cursor()
    DR_dic = []
    cur3 = conn.cursor()
    cur4 = conn.cursor()
    i=0
    wb = xlwt.Workbook()
    ws = wb.add_sheet('sheet1', cell_overwrite_ok=True)
    wp = xlwt.Pattern()
    wp.pattern = xlwt.Pattern.SOLID_PATTERN
    ws.write(0, 0, 'Table')
    ws.write(0, 1, 'duplicate_count')
    ws.write(0, 2, 'sum_count')
    ws.write(0, 3, 'duplicate_rate')
    ws.col(0).width = 8888
    ws.col(1).width = 8888
    ws.col(2).width = 4444
    ws.col(3).width = 4444
    ws.panes_frozen = True
    ws.horz_split_pos = 1
    for table in table_list:  # 对每一个表 a
        cols=[]#存放除主键外的所有字段名
        cols0=list(table_list[table]['cols'])#将表的所有列名转化为列表存入cols0中
        
        cur3.execute("SELECT COUNT(*) FROM C##SCYW."+table)
        sum_count=cur3.fetchone()[0]#计算总记录数
        if sum_count==0:
            continue
        else:
            PK=list(table_list[table]['primary_key'])#将主键转化为列表存入PK中
            
            if len(PK)==0:#没有主键的情况
                cols=cols0
            else:
                cols=list(set(cols0)-set(PK))#在所有字段名中除去主键列
                if len(cols)!=0:#防止cols为空
                    cols=','.join(str(i) for i in set(cols))#转化成带逗号的字符如 c1，c2，c3
                    
                    cur4.execute(
                                "SELECT COUNT(*) FROM C##SCYW." + table + " WHERE ("+cols+") IN (SELECT "+cols+" FROM C##SCYW." + table +" GROUP BY " +cols+ " HAVING  COUNT(*) > 1)")
                    duplicate_count=cur4.fetchone()[0]#计算重复记录数
                    if duplicate_count>0:
                        print(cols0)
                        print(PK)
                        print(cols)
                        print(table)
                        print(duplicate_count)
                        print(sum_count)
                    tmp={
                       'table':table,
                       'duplicate_count':duplicate_count,
                       'sum_count':sum_count,
                       'duplicate_rate':round(duplicate_count/sum_count,4)
                     }
                    DR_dic.append(tmp)
                    i=i+1
                    ws.write(i, 0, tmp['table'])
                    ws.write(i, 1, tmp['duplicate_count'])
                    ws.write(i, 2, tmp['sum_count'])
                    ws.write(i, 3, tmp['duplicate_rate'])
                
                else:
                    continue
    wb.save('./记录重复率./' + module_pre + '记录重复率.xls')       
    cur.close()  # 关闭cursor
    conn.close()  # 关闭连接                       
    return DR_dic

module_pre='T_DW'
print('开始判断主键')
table_list = FIND_PK(module_pre)
#print(table_list)
print('开始计算记录重复率')
print(Duplicate_Rate(table_list))

