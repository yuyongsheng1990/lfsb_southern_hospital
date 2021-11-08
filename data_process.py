# _*_ coding: utf-8 _*_
# @Time: 2021/10/27 17:51
# @Author: yuyongsheng
# @Software: PyCharm
# @Description: 南方医院利伐沙班(1)：数据处理。以case_no为基础，多次入院数据；其他联合用药

import pandas as pd
pd.set_option('mode.chained_assignment', None)
import numpy as np
import os
project_path=os.getcwd()

# 字符串转换为时间格式
import datetime
def str_to_datetime(x):
    try:
        a = datetime.datetime.strptime(x, "%d/%m/%Y %H:%M:%S")
        return a
    except:
        return np.NaN

# 1. 提取服用利伐沙班非瓣膜房颤患者
print('-------------------------1.提取提取服用利伐沙班的非瓣膜房颤患者------------------------------')
# 1.1 服用利伐沙班且出院记录中有房颤的患者
print('-------------------------提取服用利伐沙班的患者------------------------------')
# 提取服药利伐沙班的患者id
df_doctor_order=pd.read_csv(project_path+'/data/raw_data/2-doctor_order.csv')
print(df_doctor_order.shape)
df_lfsb=df_doctor_order[df_doctor_order['drug_name'].str.contains('利伐沙班')]
df_lfsb['start_datetime']=df_lfsb['start_datetime'].apply(str_to_datetime)
df_lfsb['end_datetime']=df_lfsb['end_datetime'].apply(str_to_datetime)
df_lfsb=df_lfsb.reset_index(drop=True)
print(df_lfsb.shape)
print(df_lfsb['patient_id'].nunique())
print(df_lfsb['case_no'].nunique())
# 提取用药状态为停止的利伐沙班用药，并删除服药方式为“取药用”的样本
df_lfsb=df_lfsb[(df_lfsb['statusdesc']=='停止') & (df_lfsb['medication_way']!='取药用')]
# 排序
df_lfsb=df_lfsb.sort_values(['patient_id','case_no','start_datetime'])
df_lfsb=df_lfsb.reset_index(drop=True)
df_lfsb=df_lfsb[['patient_id','case_no','drug_name','dosage','frequency','start_datetime','end_datetime']]
print(df_lfsb.shape)
print(df_lfsb['patient_id'].nunique())
print(df_lfsb['case_no'].nunique())

writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.1_利伐沙班用药记录.xlsx')
df_lfsb.to_excel(writer)
writer.save()

# 1.2 根据郑-诊断.xlsx，提取出院诊断房颤患者case_no，已进行合并纳入
print('-------------------------提取出院诊断房颤患者------------------------------')
df_diagnostic=pd.read_csv(project_path+r'/data/raw_data/3-diagnostic_record.csv')
df_oup_fib=df_diagnostic[(df_diagnostic['diagnostic_type']=='出院诊断') & (df_diagnostic['diagnostic_content'].str.contains(
'房颤射消融术后|心房扑动射频消融术后|心房颤动|阵发性心房颤动|持续性心房颤动|阵发性房颤|频发房性早搏|阵发性心房扑动|心房扑动|持续性房颤|房颤伴快速心室率\
|房颤射频消融术后|射频消融术后|快慢综合征|左心耳封堵术后|阵发性心房纤颤|心房颤动伴快速心室率|房颤|心房颤动射频消融术后|射频消融+左心耳封堵术后|左心耳封闭术后\
|心房颤动射频消融术后+左心耳封堵术|动态心电图异常：阵发性房颤、偶发房性早搏、偶发室性早搏、T波间歇性异常改变|左心房房颤射频消融+左心耳切除术后|永久性房颤\
|阵发性房颤射频消融术后|冷冻射频消融术后|心房颤动药物复律后'))]
# 调整时间格式
df_oup_fib['record_date']=df_oup_fib['record_date'].apply(str_to_datetime)
df_oup_fib=df_oup_fib.sort_values(by=['patient_id','case_no','record_date'],ascending=[True,True,True])
df_oup_fib=df_oup_fib.reset_index(drop=True)
print(df_oup_fib.shape)
print(df_oup_fib['patient_id'].nunique())
print(df_oup_fib['case_no'].nunique())
# 合并同一天的多次房颤诊断记录。
df_oup_fib['record_date_dup'] = df_oup_fib['record_date'].astype('str').apply(lambda x: x.split(' ')[0])
temp_list=[]
for i in np.unique(df_oup_fib['case_no']):
    temp=df_oup_fib[df_oup_fib['case_no']==i]
    temp=temp.reset_index(drop=True)
    j=0
    while j < temp.shape[0]-1:
        if temp.loc[j,'record_date_dup']==temp.loc[j+1,'record_date_dup']:
            # print(i)
            temp.loc[j+1,'diagnostic_content']=temp.loc[j,'diagnostic_content'] +';'+temp.loc[j+1,'diagnostic_content']
            temp=temp.drop(index=[j],axis=0)
            temp=temp.reset_index(drop=True)
        j+=1
    temp_list.append(temp)
df_oup_fib=temp_list[0]
for j in range(1,len(temp_list)):
    df_oup_fib=pd.concat([df_oup_fib,temp_list[j]],axis=0)
df_oup_fib=df_oup_fib.reset_index(drop=True)
df_oup_fib=df_oup_fib[['patient_id','case_no','record_date','diagnostic_content']]
df_oup_fib['case_no']=df_oup_fib['case_no'].astype('str')
print(df_oup_fib.shape)
print(df_oup_fib['patient_id'].nunique())
writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.2_出院诊断房颤患者记录.xlsx')
df_oup_fib.to_excel(writer)
writer.save()

# 统计
# df_lfsb_stats=pd.merge(df_lfsb,df_oup_fib,on=['case_no'],how='inner')
# print(df_lfsb_stats.shape)
# print(df_lfsb_stats['patient_id'].nunique())
# print(df_lfsb_stats['case_no'].nunique())

# 1.3 提取瓣膜性房颤患者：手术中有膜瓣置换、诊断中为瓣膜性房颤。
print('-------------------------排除瓣膜置换手术-----------------------------')
# 根据郑-手术.xlsx，排除膜瓣置换手术
df_surgical_record=pd.read_csv(project_path+'/data/raw_data/1-surgical_record.csv')
df_surgical_valve=df_surgical_record[df_surgical_record['surgery_name'].str.contains('瓣膜|膜瓣')]
print(df_surgical_valve.shape)
print(df_surgical_valve['patient_id'].nunique())
print(df_surgical_valve['case_no'].nunique())

# 排除临床诊断中瓣膜性房颤，包含：心脏瓣膜病和风湿性瓣膜病；不包括下肢静脉瓣膜病
print('-------------------------排除瓣膜性房颤患者-----------------------------')
df_test_record=pd.read_csv(project_path+'/data/raw_data/4-test_record.csv')
df_test_record=df_test_record[df_test_record['clinical_diagnosis'].notnull()]  # 非空
df_heart_valve=df_test_record[df_test_record['clinical_diagnosis'].str.contains('瓣膜')]
df_heart_valve=df_heart_valve[df_heart_valve['clinical_diagnosis'].str.contains('心脏|风湿性')]
df_heart_valve['case_no']=df_heart_valve['case_no'].astype('str')
print(df_heart_valve.shape)
print(df_heart_valve['patient_id'].nunique())
print(df_heart_valve['case_no'].nunique())

# 1.4服用利伐沙班的非瓣膜患者。利伐沙班用药患者&出院房颤诊断-瓣膜房颤
print('-----------------------服用利伐沙班的非瓣膜患者---------------------------')
lfsb_list=list(np.unique(df_lfsb['patient_id']))
oup_fib_list=list(np.unique(df_oup_fib['patient_id']))
surgical_valve_list=list(np.unique(df_surgical_valve['patient_id']))
heart_valve_list=list(np.unique(df_heart_valve['patient_id']))
# 提取服用利伐沙班的非瓣膜患者id列表
lfsb_not_valve_list=[x for x in lfsb_list if x in oup_fib_list if x not in surgical_valve_list if x not in heart_valve_list]
df_lfsb_not_valve=df_lfsb[df_lfsb['patient_id'].isin(lfsb_not_valve_list)]
df_lfsb_not_valve=df_lfsb_not_valve.reset_index(drop=True)
extract_field=['patient_id','case_no','drug_name','dosage','frequency','start_datetime','end_datetime']
df_lfsb_not_valve=df_lfsb_not_valve[extract_field]
print(df_lfsb_not_valve.shape)
print(df_lfsb_not_valve['patient_id'].nunique())
print(df_lfsb_not_valve['case_no'].nunique())

writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.4_服用利伐沙班的非瓣膜房颤患者.xlsx')
df_lfsb_not_valve.to_excel(writer)
writer.save()

# 1.5计算利伐沙班用药日剂量
print('-------------------------计算出院时利伐沙班用药日剂量------------------------------')
print(np.unique(df_lfsb_not_valve['frequency']))
# 一片利伐沙班10mg
df_lfsb_not_valve['dosage']=df_lfsb_not_valve['dosage'].apply(lambda x: x.replace('mg', '') if 'mg' in x else 10 if '片' in x else x)
third=['1/72小时']
half=['1/2日','1/隔日']
one=['1/午','1/单日','1/日','1/日(餐前)','1/早','1/晚','Qd','Qd(8am)']
two=['1/12小时','12/日','2/日']
three=['Tid']
df_lfsb_not_valve['frequency']=df_lfsb_not_valve['frequency'].apply(lambda x: 0.33 if x in third else
                                                        0.5 if x in half else
                                                        1 if x in one else
                                                        2 if x in two else
                                                        3 if x in three else x)
df_lfsb_not_valve['日剂量']=df_lfsb_not_valve['dosage'].astype('float') * df_lfsb_not_valve['frequency'].astype('float')

# 合并同一case_no的多次用药数据，取最后一次日剂量作为最终日剂量
temp_list=[]
for i in np.unique(df_lfsb_not_valve['case_no']):
    temp=df_lfsb_not_valve[df_lfsb_not_valve['case_no']==i]
    temp=temp.reset_index(drop=True)
    if temp.shape[0]>1:
        temp.loc[0,'日剂量']=temp.loc[(temp.shape[0]-1),'日剂量']
    temp_list.append(temp)
df_lfsb_not_valve=temp_list[0]
for j in range(1,len(temp_list)):
    df_lfsb_not_valve=pd.concat([df_lfsb_not_valve,temp_list[j]],axis=0)
df_lfsb_not_valve=df_lfsb_not_valve.reset_index(drop=True)
df_lfsb_not_valve=df_lfsb_not_valve[['patient_id','case_no','drug_name','start_datetime','end_datetime','日剂量']]
df_lfsb_not_valve=df_lfsb_not_valve.drop_duplicates(subset=['case_no'],keep='first')
print(df_lfsb_not_valve.shape)
print(df_lfsb_not_valve['patient_id'].nunique())
print(df_lfsb_not_valve['case_no'].nunique())
writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.4_计算出院事利伐沙班日剂量.xlsx')
df_lfsb_not_valve.to_excel(writer)
writer.save()

# 1.6 合并人口信息学数据
print('-------------------------合并人口信息学数据-----------------------------')
df_popu=pd.read_excel(project_path+'/data/raw_data/1.基本信息(诊断非瓣膜房颤用利伐沙班).xlsx')
if 'Unnamed: 0' in df_popu.columns:
    df_popu = df_popu.drop(['Unnamed: 0'], axis=1)
df_popu=df_popu[['case_no','gender','age','height','weight','BMI']]
# 删除人口信息学重复数据，只保留第一条
df_popu=df_popu.drop_duplicates(subset=['case_no'],keep='first')
# case_no分类更细
df_lfsb_not_valve=pd.merge(df_lfsb_not_valve,df_popu,on=['case_no'],how='left')
print(df_lfsb_not_valve.shape)
print(df_lfsb_not_valve['patient_id'].nunique())
print(df_lfsb_not_valve['case_no'].nunique())
# 补充缺失的性别、年龄、身高信息
# 读取patient_info-包含性别和年龄；patient_sign_record-包含身高
df_patient_info=pd.read_csv(project_path+'/data/raw_data/1-patient_info.csv')
df_patient_info = df_patient_info.set_index('patient_id')
df_patient_sign_record=pd.read_csv(project_path+'/data/raw_data/1-patient_sign_record.csv')
df_height = df_patient_sign_record[df_patient_sign_record['sign_type'] == '身高(cm)']

aaa=df_lfsb_not_valve[df_lfsb_not_valve['gender'].isnull()]
bbb=df_lfsb_not_valve[df_lfsb_not_valve['gender'].notnull()]
aaa_list=[]
for i in np.unique(aaa['patient_id']):
    # print(i)
    temp=aaa[aaa['patient_id']==i]
    temp=temp.reset_index(drop=True)
    # 提取缺失的性别数据
    gender=df_patient_info.loc[i,'gender']
    if gender =='男':
        gender_value=1
    else:
        gender_value=0
    temp['gender']=gender_value
    # 提取缺失的年龄数据
    age=df_patient_info.loc[i,'birth_year']
    age_year=age.split('-')[0]
    start_datetime=temp.loc[0,'start_datetime']
    start_year=str(start_datetime).split('-')[0]
    # start_year=start_time[0:3]
    age_value=int(start_year)-int(age_year)
    temp['age']=age_value
    # 提取身高信息
    height= df_height[df_height['patient_id']==i]
    height=height.reset_index(drop=True)
    height=height.loc[0,'record_content']
    if height=='卧床' or height=='轮椅':
        temp['height']=np.nan
    else:
        temp['height']=height
    aaa_list.append(temp)
aaa=aaa_list[0]
for j in range(1,len(aaa_list)):
    aaa=pd.concat([aaa,aaa_list[j]],axis=0)
df_lfsb_not_valve=pd.concat([aaa,bbb],axis=0)
df_lfsb_not_valve=df_lfsb_not_valve.sort_values(['patient_id'])
df_lfsb_not_valve=df_lfsb_not_valve.reset_index(drop=True)
print(df_lfsb_not_valve.shape)
print(df_lfsb_not_valve['patient_id'].nunique())
print(df_lfsb_not_valve['case_no'].nunique())
# 统计年龄分布
df_age_stats=df_lfsb_not_valve.drop_duplicates(subset=['patient_id'],keep='first')
print(df_age_stats['age'].describe())
writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.5_合并人口信息学特征的非瓣膜房颤患者.xlsx')
df_lfsb_not_valve.to_excel(writer)
writer.save()

# 获取df_test
df_test_record=pd.read_csv(project_path+'/data/raw_data/4-test_record.csv')
df_test_record=df_test_record[['test_record_id','patient_id','test_date']]
df_test_result=pd.read_csv(project_path+'/data/raw_data/4-test_result.csv')
df_test_result=df_test_result[['test_record_id','project_name','test_result']]
df_test=pd.merge(df_test_record,df_test_result,on=['test_record_id'],how='inner')
# 删除<>号
df_test['test_result']=df_test['test_result'].astype('str').apply(lambda x:x.replace('<',''))
df_test['test_result']=df_test['test_result'].astype('str').apply(lambda x:x.replace('>',''))
df_lfsb_test=pd.merge(df_lfsb_not_valve,df_test,on=['patient_id'],how='left')
# 统计血小板分布
df_platelet=df_lfsb_test[df_lfsb_test['project_name']=='血小板计数']
df_platelet=df_platelet[df_platelet['test_result'].astype('int')<10]
df_platelet=df_platelet.drop_duplicates(subset=['patient_id'],keep='first')
print(df_platelet['test_result'].describe())
# 统计凝血时间异常

print('---------------------添加联合用药和其他检验信息------------------------------')
# 添加高血压、糖尿病特征，根据diagnostic_record文件和梦璇文件总数据20210607.xlsx
df_mx=pd.read_excel(project_path+'/data/raw_data/总数据20210607.xlsx')
if 'Unnamed: 0' in df_lfsb.columns:
    df_lfsb = df_lfsb.drop(['Unnamed: 0'], axis=1)
temp_list=[]
for i in np.unique(df_lfsb_not_valve['case_no']):
    temp=df_lfsb_not_valve[df_lfsb_not_valve['case_no']==i]
    for j in np.unique(df_mx.columns[12:]):
        temp[j]=df_mx[j]
    temp_list.append(temp)
df_lfsb_not_valve=temp_list[0]
for j in range(1,len(temp_list)):
    df_lfsb_not_valve=pd.concat([df_lfsb_not_valve,temp_list[j]],axis=0)
df_lfsb_not_valve=df_lfsb_not_valve.reset_index(drop=True)
print(df_lfsb_not_valve.shape)
print(df_lfsb_not_valve['case_no'].nunique())
writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.6_合并用药和检验信息.xlsx')
df_lfsb_not_valve.to_excel(writer)
writer.save()

# 入院诊断: 补充诊断、初步诊断、门诊诊断、修正诊断、最后诊断
print('-------------------------提取入院诊断-----------------------------')
df_diagnostic_inp=df_diagnostic[df_diagnostic['diagnostic_type'].str.contains('补充诊断|初步诊断|门诊诊断|修正诊断|最后诊断')]
# 删除空值
df_diagnostic_inp=df_diagnostic_inp[df_diagnostic_inp['case_no'].notnull()]
# 入院诊断case_no格式调整：由float转为str
df_diagnostic_inp['case_no']=df_diagnostic_inp['case_no'].astype('str')
df_diagnostic_inp['case_no']=df_diagnostic_inp['case_no'].apply(lambda x: x.split('\.')[0])
df_diagnostic_inp=df_diagnostic[['patient_id','case_no','record_date','diagnostic_type','diagnostic_content']]
# 合并同一case_no的入院诊断
temp_list=[]
for i in np.unique(df_diagnostic_inp['case_no']):
    temp=df_diagnostic_inp[df_diagnostic_inp['case_no']==i]
    temp=temp.reset_index(drop=True)
    j=0
    while j < temp.shape[0]-1:
        # print(i)
        temp.loc[j+1,'diagnostic_content']=temp.loc[j,'diagnostic_content'] +';'+temp.loc[j+1,'diagnostic_content']
        temp=temp.drop(index=[j],axis=0)
        temp=temp.reset_index(drop=True)
        temp=temp.drop_duplicates(subset=['case_no'],keep='last')
        j+=1
    temp_list.append(temp)
df_diagnostic_inp=temp_list[0]
for j in range(1,len(temp_list)):
    df_diagnostic_inp=pd.concat([df_diagnostic,temp_list[j]],axis=0)
del temp_list
df_diagnostic_inp=df_diagnostic_inp.reset_index(drop=True)
print(df_diagnostic_inp.shape)
print(df_diagnostic_inp['case_no'].nunique())
writer=pd.ExcelWriter(project_path+'/data/processed_data/df_2.1_再次入院诊断信息.xlsx')
df_diagnostic_inp.to_excel(writer)
writer.save()

# 2.计算多次出院时间，case_no
df_inp_record=pd.read_csv(project_path+'/data/raw_data/1-inp_record.csv')
df_inp_record=df_inp_record[df_inp_record['adm_date'].notnull() & df_inp_record['dis_date'].notnull()]
df_inp_record['adm_date']=df_inp_record['adm_date'].apply(str_to_datetime)
df_inp_record['dis_date']=df_inp_record['dis_date'].apply(str_to_datetime)
df_inp_record=df_inp_record.sort_values(by=['case_no','adm_date'])
print('-------------------------计算多次出院时间-----------------------------')
temp_list=[]
for i in np.unique(df_lfsb_not_valve['patient_id']):
    temp=df_lfsb_not_valve[df_lfsb_not_valve['patient_id']==i]
    temp=temp.reset_index(drop=True)
    temp_inp=df_inp_record[df_inp_record['patient_id']==i]
    temp_inp=temp_inp.reset_index(drop=True)
    # 判断是否存在多次入院信息
    if temp_inp.shape[0]>1:
        for j in range(0,temp_inp.shape[0]-1):
            case_no = temp_inp.loc[j, 'case_no']
            # 出院时间
            oup = temp_inp.loc[j, 'dis_date']
            oup_time = '第%s次出院' % (j + 1)
            temp[oup_time] = oup
            inp = temp_inp.loc[j + 1, 'adm_date']
            # 入院时间和诊断
            inp_time = '第%s次入院' % (j + 2)
            diagnostic_inp = '第%s次入院诊断' % (j + 2)
            diagnostic_inp_content = df_diagnostic_inp[case_no]
            temp[inp_time] = inp
            temp[diagnostic_inp] = diagnostic_inp
            temp[diagnostic_inp_content] = diagnostic_inp_content
            # 出院到入院时间间隔
            interval_time = inp - oup
            interval_days = interval_time.days
            temp[oup_time + '-' + inp_time] = interval_days
    temp_list.append(temp)
df_lfsb_inp=temp_list[0]
for k in range(1,len(temp_list)):
    df_lfsb_inp=pd.concat([df_lfsb_inp,temp_list[k]],axis=0)
df_lfsb_inp=df_lfsb_inp.sort_values(['case_no','start_datetime'])
df_lfsb_inp=df_lfsb_inp.reset_index(drop=True)

writer=pd.ExcelWriter(project_path+'/data/processed_data/df_2.2_计算多次出院时间.xlsx')
df_lfsb_inp.to_excel(writer)
writer.save()

# 3.按剂量10、15、20分组，计算再次入院率
df_inp_record=pd.read_csv(project_path+'/data/raw_data/1-inp_record.csv')
df_inp_record=df_inp_record[df_inp_record['adm_date'].notnull() & df_inp_record['dis_date'].notnull()]
df_inp_record['adm_date']=df_inp_record['adm_date'].apply(str_to_datetime)
df_inp_record['dis_date']=df_inp_record['dis_date'].apply(str_to_datetime)
df_inp_record=df_inp_record.sort_values(by=['case_no','adm_date'])
df_lfsb_not_valve['剂量分组']=df_lfsb_not_valve['日剂量'].apply(lambda x: 0 if x==10 else 1 if x==15 else 2 if x==20 else np.nan)
df_lfsb_0=df_lfsb_not_valve[df_lfsb_not_valve['剂量分组']==0]
df_lfsb_1=df_lfsb_not_valve[df_lfsb_not_valve['剂量分组']==1]
df_lfsb_2=df_lfsb_not_valve[df_lfsb_not_valve['剂量分组']==2]
# 统计分组数
num_10=df_lfsb_0['patient_id'].nunique()
num_15=df_lfsb_1['patient_id'].nunique()
num_20=df_lfsb_2['patient_id'].nunique()
print('分组人数',num_10,num_15,num_20)
#统计再次入院的每组人数
count_10=0
for i in np.unique(df_lfsb_0['patient_id']):
    temp=df_lfsb_0[df_lfsb_0['patient_id']==i]
    temp=temp.reset_index(drop=True)
    if temp.shape[0]>1:
        count_10 +=1
count_15=0
for i in np.unique(df_lfsb_1['patient_id']):
    temp=df_lfsb_1[df_lfsb_1['patient_id']==i]
    temp=temp.reset_index(drop=True)
    if temp.shape[0]>1:
        count_15 +=1
count_20=0
for i in np.unique(df_lfsb_2['patient_id']):
    temp=df_lfsb_2[df_lfsb_2['patient_id']==i]
    temp=temp.reset_index(drop=True)
    if temp.shape[0]>1:
        count_20 +=1
print('10mg再次入院',count_10,count_10/num_10)
print('15mg再次入院',count_15,count_15/num_10)
print('20mg再次入院',count_20,count_20/num_10)










