# _*_ coding: utf-8 _*_
# @Time: 2021/10/27 17:51
# @Author: yuyongsheng
# @Software: PyCharm
# @Description: 南方医院利伐沙班(1)：数据处理。多次入院数据；其他联合用药；lfsb用药1/3/7/14/28天的检验信息，以WBC和CRP为例。

import pandas as pd
pd.set_option('mode.chained_assignment', None)
import numpy as np
import os
project_path=os.getcwd()

# 1. 提取服用利伐沙班非瓣膜房颤患者
print('-------------------------提取提取服用利伐沙班的非瓣膜房颤患者------------------------------')
# 1.1 服用利伐沙班且出院记录中有房颤的患者
print('-------------------------提取服用利伐沙班的患者------------------------------')
# 提取服药利伐沙班的患者id
df_doctor_order=pd.read_csv(project_path+'/data/raw_data/2-doctor_order.csv')
print(df_doctor_order.shape)
# df_lfsb=df_doctor_order[df_doctor_order['patient_id']==7997945]
df_lfsb=df_doctor_order[df_doctor_order['drug_name'].str.contains('利伐沙班')]
df_lfsb=df_lfsb.reset_index(drop=True)
print(df_lfsb.shape)
print(df_lfsb['patient_id'].nunique())
# 提取用药状态为停止的利伐沙班用药，并删除服药方式为“取药用”的样本
df_lfsb=df_lfsb[(df_lfsb['statusdesc']=='停止') & (df_lfsb['medication_way']!='取药用')]
# 排序
df_lfsb=df_lfsb.sort_values(['patient_id','start_datetime'],ascending=[True,True])
df_lfsb=df_lfsb.reset_index(drop=True)
print(df_lfsb.shape)
print(df_lfsb['patient_id'].nunique())

writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.1_利伐沙班用药记录.xlsx')
df_lfsb.to_excel(writer)
writer.save()
# 1.2计算利伐沙班用药日剂量
print('-------------------------计算利伐沙班用药日剂量------------------------------')
print(np.unique(df_lfsb['frequency']))
# 一片利伐沙班10mg
df_lfsb['dosage']=df_lfsb['dosage'].apply(lambda x: x.replace('mg', '') if 'mg' in x else 10 if '片' in x else x)
third=['1/72小时']
half=['1/2日','1/隔日']
one=['1/午','1/单日','1/日','1/日(餐前)','1/早','1/晚','Qd','Qd(8am)']
two=['1/12小时','12/日','2/日']
three=['Tid']
df_lfsb['frequency']=df_lfsb['frequency'].apply(lambda x: 0.33 if x in third else
                                                        0.5 if x in half else
                                                        1 if x in one else
                                                        2 if x in two else
                                                        3 if x in three else x)
df_lfsb['日剂量']=df_lfsb['dosage'].astype('float') * df_lfsb['frequency'].astype('float')
# 合并重复同一天多次数据
print('-------------------------合并利伐沙班用药同一天多次数据------------------------------')
# 整理异常用药数据：同一天内多次短时用药；短时用药与长时用药交叉。短时用药：start_datetime=end_datetime；长时用药：start_datetime<end_datetime。
# 同一天内存在多次短时用药，将同一天内的多次用药相加，id:7997945；
# 若短时用药与长时用药在同一天交叉，将长时用药start_datetime往后加一天。如果因此长时用药变成新短时用药，并与第二天的短时用药重复或长时用药交叉，则继续相加或往后加一天，id:958783。
import datetime

# 字符串转换为时间格式
import datetime
def str_to_datetime(x):
    try:
        a = datetime.datetime.strptime(x, "%d/%m/%Y %H:%M:%S")
        return a
    except:
        return np.NaN
# 时间格式转化
def time_format_trans(t):
    try:
        t_0=str(t).split(' ')[0]
        t_1=str(t).split(' ')[1]
        t_0_list=t_0.split('-')
        year=t_0_list[-3]
        mon=t_0_list[-2]
        if mon.startswith('0'):
            mon=mon[-1]
        day=t_0_list[-1]
        if day.startswith('0'):
            day=day[-1]
        t_new=day+'/'+mon+'/'+year+' '+t_1
        return t_new
    except:
        return np.nan
temp_list=[]
for i in np.unique(df_lfsb['patient_id']):
    temp=df_lfsb[df_lfsb['patient_id']==i]
    temp = temp.sort_values(['start_datetime'], ascending=True)
    temp=temp.reset_index(drop=True)
    if temp.shape[0] > 1:
        # 如果两条用药start_datetime和end_datetime分别相同，则累加为一条
        j=0
        while j < temp.shape[0]-1:
            if (temp.loc[j,'start_datetime']==temp.loc[j+1,'start_datetime']) & (temp.loc[j,'end_datetime']==temp.loc[j+1,'end_datetime']):
                # print(i)
                temp.loc[j,'日剂量'] = temp.loc[j,'日剂量'] + temp.loc[j+1,'日剂量']
                temp=temp.drop(index=[j+1],axis=0)
            j +=1
        temp['start_datetime_temp']=temp['start_datetime'].apply(lambda x: x.split(' ')[0])
        temp['end_datetime_temp']=temp['end_datetime'].apply(lambda x: x.split(' ')[0])
        j=0
        while j < temp.shape[0]-1:
            # 存在短时用药
            if temp.loc[j,'start_datetime_temp']==temp.loc[j,'end_datetime_temp']:
                # 如果存在多次短时用药
                if (temp.loc[j+1,'start_datetime_temp']==temp.loc[j+1,'end_datetime_temp'])&(temp.loc[j,'start_datetime_temp']==temp.loc[j+1,'start_datetime_temp']):
                    temp.loc[j+1,'日剂量'] = temp.loc[j,'日剂量']+temp.loc[j+1,'日剂量']
                    temp=temp.drop(index=[j],axis=0)
                    temp=temp.reset_index(drop=True)
                    continue
                # 如果存在交叉长时用药
                elif (temp.loc[j+1,'start_datetime_temp']!=temp.loc[j+1,'end_datetime_temp'])&(temp.loc[j,'start_datetime_temp']==temp.loc[j+1,'start_datetime_temp']):
                    temp.loc[j+1,'start_datetime']= time_format_trans(str_to_datetime(temp.loc[j+1,'start_datetime']) + datetime.timedelta(days=1))
                    temp.loc[j+1,'start_datetime_temp']= str(temp.loc[j+1,'start_datetime']).split(' ')[0]
            j+=1
    temp_list.append(temp)
df_lfsb=temp_list[0]
for j in range(1,len(temp_list)):
    df_lfsb=pd.concat([df_lfsb,temp_list[j]],axis=0)
df_lfsb=df_lfsb.reset_index(drop=True)
print(df_lfsb.shape)
print(df_lfsb['patient_id'].nunique())
writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.2_计算利伐沙班日剂量.xlsx')
df_lfsb.to_excel(writer)
writer.save()

# 1.3 提取出院诊断房颤患者id
print('-------------------------提取出院诊断房颤患者------------------------------')
df_diagnostic=pd.read_csv(project_path+'/data/raw_data/3-diagnostic_record.csv')
df_oup_fib=df_diagnostic[(df_diagnostic['diagnostic_type']=='出院诊断') & (df_diagnostic['diagnostic_content'].str.contains('房颤'))]
df_oup_fib=df_oup_fib.sort_values(by=['patient_id','record_date'],ascending=[True,True])
df_oup_fib=df_oup_fib.reset_index(drop=True)
print(df_oup_fib.shape)
print(df_oup_fib['patient_id'].nunique())
# 合并同一天的多次房颤诊断记录。
df_oup_fib['record_date_dup'] = df_oup_fib['record_date'].apply(lambda x: x.split(' ')[0])
temp_list=[]
for i in np.unique(df_oup_fib['patient_id']):
    temp=df_oup_fib[df_oup_fib['patient_id']==i]
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
df_oup_fib=df_oup_fib.drop(['record_date_dup'],axis=1)
print(df_oup_fib.shape)
print(df_oup_fib['patient_id'].nunique())
writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.3_出院诊断房颤患者记录.xlsx')
df_oup_fib.to_excel(writer)
writer.save()

# 1.4提取瓣膜性房颤患者：手术中有膜瓣置换、诊断中为瓣膜性房颤。
print('-------------------------排除瓣膜性房颤患者-----------------------------')
# 手术中有膜瓣置换
df_surgical_record=pd.read_csv(project_path+'/data/raw_data/1-surgical_record.csv')
df_surgical_valve=df_surgical_record[df_surgical_record['surgery_name'].str.contains('瓣膜|膜瓣')]
print(df_surgical_valve.shape)
print(df_surgical_valve['patient_id'].nunique())

# 临床诊断中瓣膜性房颤，包含：心脏瓣膜病和风湿性瓣膜病；不包括下肢静脉瓣膜病
df_test_record=pd.read_csv(project_path+'/data/raw_data/4-test_record.csv')
df_test_record=df_test_record[df_test_record['clinical_diagnosis'].notnull()]  # 非空
df_heart_valve=df_test_record[df_test_record['clinical_diagnosis'].str.contains('瓣膜')]
df_heart_valve=df_heart_valve[df_heart_valve['clinical_diagnosis'].str.contains('心脏|风湿性')]
print(df_heart_valve.shape)
print(df_heart_valve['patient_id'].nunique())

# 1.5服用利伐沙班的非瓣膜患者。利伐沙班用药患者&出院房颤诊断-瓣膜房颤
print('-------------------------排除瓣膜性房颤患者-----------------------------')
lfsb_list=list(np.unique(df_lfsb['patient_id']))
oup_fib_list=list(np.unique(df_oup_fib['patient_id']))
surgical_valve_list=list(np.unique(df_surgical_valve['patient_id']))
heart_valve_list=list(np.unique(df_heart_valve['patient_id']))
# 提取服用利伐沙班的非瓣膜患者id列表
lfsb_not_valve_list=[x for x in lfsb_list if x in oup_fib_list if x not in surgical_valve_list if x not in heart_valve_list]
df_lfsb_not_valve=df_lfsb[df_lfsb['patient_id'].isin(lfsb_not_valve_list)]
df_lfsb_not_valve=df_lfsb_not_valve.reset_index(drop=True)
extract_field=['patient_id','case_no','drug_name','dosage','frequency','日剂量','start_datetime','end_datetime']
df_lfsb_not_valve=df_lfsb_not_valve[extract_field]
print(df_lfsb_not_valve.shape)
print(df_lfsb_not_valve['patient_id'].nunique())

writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.4_服用利伐沙班的非瓣膜房颤患者.xlsx')
df_lfsb_not_valve.to_excel(writer)
writer.save()

# 1.6 计算患者用药总剂量
print('-------------------------计算患者用药总剂量-----------------------------')
# 调整时间类型
df_lfsb_not_valve['start_datetime']=df_lfsb_not_valve['start_datetime'].astype('str').apply(str_to_datetime)
df_lfsb_not_valve['end_datetime']=df_lfsb_not_valve['end_datetime'].astype('str').apply(str_to_datetime)
df_lfsb_not_valve=df_lfsb_not_valve.sort_values(['case_no','start_datetime'])
for i in range(df_lfsb_not_valve.shape[0]):
    inter_days=df_lfsb_not_valve.loc[i,'end_datetime'] - df_lfsb_not_valve.loc[i,'start_datetime']
    df_lfsb_not_valve.loc[i,'服药时长']=inter_days.days+1
    df_lfsb_not_valve.loc[i,'总剂量']=df_lfsb_not_valve.loc[i,'日剂量'] * (inter_days.days + 1)
writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.5_计算患者用药总剂量.xlsx')
df_lfsb_not_valve.to_excel(writer)
writer.save()

# 1.5 合并人口信息学数据
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

writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.6_合并人口信息学特征的非瓣膜房颤患者.xlsx')
df_lfsb_not_valve.to_excel(writer)
writer.save()

# 1.7 计算住院期间日均剂量
print('-----------------------计算住院期间日均剂量-----------------------------')
df_inp_record=pd.read_csv(project_path+'/data/raw_data/1-inp_record.csv')
df_inp_record=df_inp_record[df_inp_record['adm_date'].notnull() & df_inp_record['dis_date'].notnull()]
df_inp_record['adm_date']=df_inp_record['adm_date'].apply(str_to_datetime)
df_inp_record['dis_date']=df_inp_record['dis_date'].apply(str_to_datetime)
df_inp_record=df_inp_record.sort_values(by=['case_no','adm_date'])

df_inp_time=df_inp_record[['case_no','adm_date','dis_date']]
# case_no是患者住院id，很好用
temp_list=[]
for i in np.unique(df_inp_time['case_no']):
    print(i)
    temp=df_lfsb_not_valve[df_lfsb_not_valve['case_no']==i]
    temp=temp.reset_index(drop=True)
    temp_inp=df_inp_time[df_inp_time['case_no']==i]
    temp_inp=temp_inp.reset_index(drop=True)
    # 计算日均剂量
    dosage_all=temp['总剂量'].sum()
    days_all=temp['服药时长'].sum()
    temp['住院期间总剂量']=dosage_all
    temp['住院期间服药时长']=days_all
    temp['住院期间日均剂量']=round(dosage_all/days_all,2)
    # 住院时间
    temp=pd.merge(temp,temp_inp,on=['case_no'],how='inner')
    # 住院时长
    inp_time=temp_inp.loc[0,'dis_date']-temp_inp.loc[0,'adm_date']
    temp['住院时长']=inp_time.days
    temp_list.append(temp)

df_lfsb_not_valve=temp_list[0]
for j in range(1,len(temp_list)):
    df_lfsb_not_valve=pd.concat([df_lfsb_not_valve,temp_list[j]],axis=0)
df_lfsb_not_valve=df_lfsb_not_valve.sort_values(['case_no','start_datetime'])
df_lfsb_not_valve=df_lfsb_not_valve.reset_index(drop=True)
print(df_lfsb_not_valve.shape)
print(df_lfsb_not_valve['patient_id'].nunique())

writer=pd.ExcelWriter(project_path+'/data/processed_data/df_1.7_计算住院期间日均剂量.xlsx')
df_lfsb_not_valve.to_excel(writer)
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
            oup=temp_inp.loc[j,'dis_date']
            oup_time='第%s次出院' % (j+1)
            inp=temp_inp.loc[j+1,'adm_date']
            inp_time='第%s次入院' % (j+2)
            interval_time=inp-oup
            interval_days=interval_time.days
            temp[oup_time+'_'+inp_time]=interval_days
    temp_list.append(temp)
df_lfsb_inp=temp_list[0]
for k in range(1,len(temp_list)):
    df_lfsb_inp=pd.concat([df_lfsb_inp,temp_list[k]],axis=0)

df_lfsb_inp=df_lfsb_inp.sort_values(['case_no','start_datetime'])
df_lfsb_inp=df_lfsb_inp.reset_index(drop=True)

writer=pd.ExcelWriter(project_path+'/data/processed_data/df_2_计算多次出院时间.xlsx')
df_lfsb_inp.to_excel(writer)
writer.save()

# 3. 以CRP、WBC为例的检测信息
print('-------------------------以CRP、WBC为例的检测信息-----------------------------')

# 过滤异常值
def filter_exce_value(df,feature):
    # 过滤文字!!!!!!!!!!!!!!!!!!!!!!!!!!!
    df=df[df[feature].str.contains('\d')]
    # 过滤异常大值!!!!!!!!!!!!!!!!!!!!!!!!!!
    median_value=df[feature].median()
    df[feature]=df[feature].apply(lambda x: x if abs(float(x)) < (100 * abs(median_value)) else np.nan)
    df=df[df[feature].notnull()]
    return df

df_test_record=pd.read_csv(project_path+'/data/raw_data/4-test_record.csv')
df_test_record=df_test_record[['test_record_id','patient_id','test_date']]
df_test_result=pd.read_csv(project_path+'/data/raw_data/4-test_result.csv')
df_test_result=df_test_result[['test_record_id','project_name','test_result']]
df_test=pd.merge(df_test_record,df_test_result,on=['test_record_id'],how='inner')
# 删除<>号
df_test['test_result']=df_test['test_result'].astype('str').apply(lambda x:x.replace('<',''))
df_test['test_result']=df_test['test_result'].astype('str').apply(lambda x:x.replace('>',''))

# 计算CRP和WBC
df_test_CRP=df_test[df_test['project_name'].str.contains('C反应蛋白')]
df_test_CRP=df_test_CRP.reset_index(drop=True)
df_test_CRP=filter_exce_value(df_test_CRP,'test_result')
df_test_WBC=df_test[df_test['project_name'].str.contains('白细胞计数')]
df_test_WBC=df_test_WBC.reset_index(drop=True)
df_test_WBC=filter_exce_value(df_test_WBC,'test_result')

temp_list=[]
for i in np.unique(df_lfsb_inp['patient_id']):
    print(i)
    # 利伐沙班用药
    temp=df_lfsb_inp[df_lfsb_inp['patient_id']==i]
    # temp['start_datetime']=temp['start_datetime']
    temp=temp.sort_values(['patient_id','start_datetime'])
    temp=temp.reset_index(drop=True)
    # CRP检测
    temp_CRP=df_test_CRP[df_test_CRP['patient_id']==i]
    temp_CRP['test_date']=temp_CRP['test_date'].apply(str_to_datetime)
    temp_CRP=temp_CRP.sort_values(['patient_id','test_date'])
    temp_CRP=temp_CRP.reset_index(drop=True)
    # WBC检测
    temp_WBC=df_test_WBC[df_test_WBC['patient_id']==i]
    temp_WBC['test_date']=temp_WBC['test_date'].apply(str_to_datetime)
    temp_WBC=temp_WBC.sort_values(['patient_id','test_date'])
    temp_WBC=temp_WBC.reset_index(drop=True)
    for j in range(temp.shape[0]):
        # 提取利伐沙班服药前的CRP数值
        df_CRP_former=temp_CRP[(temp_CRP['test_date']<temp.loc[j,'start_datetime'])&
                             (temp_CRP['test_date'] >= temp.loc[j, 'start_datetime'] - datetime.timedelta(days=4))]
        CRP_former_value=df_CRP_former['test_result'].astype('float').mean()
        temp.loc[j,'CRP_former']=round(CRP_former_value,2)
        # 提取CRP第1天检测
        df_CRP_1d=temp_CRP[(temp_CRP['test_date']>=temp.loc[j,'start_datetime']) &
                           (temp_CRP['test_date']<=temp.loc[j,'start_datetime']+datetime.timedelta(days=1))]
        CRP_1d_value=df_CRP_1d['test_result'].astype('float').mean()
        temp.loc[j,'CRP_1d']=round(CRP_1d_value,2)
        # 提取CRP第1天检测与利伐沙班服药前CRP的差值
        CRP_1d_former_value=CRP_1d_value-CRP_former_value
        temp.loc[j, 'CRP_1d_former'] = round(CRP_1d_former_value, 2)
        # 提取CRP第3天检测
        df_CRP_3d=temp_CRP[(temp_CRP['test_date']>=temp.loc[j,'start_datetime'] + datetime.timedelta(days=2)) &
                           (temp_CRP['test_date']<=temp.loc[j,'start_datetime']+datetime.timedelta(days=4))]
        CRP_3d_value=df_CRP_3d['test_result'].astype('float').mean()
        temp.loc[j,'CRP_3d']=round(CRP_3d_value,2)
        # 提取CRP第3天检测与利伐沙班服药前CRP的差值
        CRP_3d_former_value=CRP_3d_value-CRP_former_value
        temp.loc[j, 'CRP_3d_former'] = round(CRP_3d_former_value, 2)
        # 提取CRP第7天检测
        df_CRP_7d=temp_CRP[(temp_CRP['test_date']>=temp.loc[j,'start_datetime'] + datetime.timedelta(days=6)) &
                           (temp_CRP['test_date']<=temp.loc[j,'start_datetime']+datetime.timedelta(days=8))]
        CRP_7d_value=df_CRP_7d['test_result'].astype('float').mean()
        temp.loc[j,'CRP_7d']=round(CRP_7d_value,2)
        # 提取CRP第7天检测与利伐沙班服药前CRP的差值
        CRP_7d_former_value=CRP_7d_value-CRP_former_value
        temp.loc[j, 'CRP_7d_former'] = round(CRP_7d_former_value, 2)
        # 提取CRP第14天检测
        df_CRP_14d=temp_CRP[(temp_CRP['test_date']>=temp.loc[j,'start_datetime'] + datetime.timedelta(days=13)) &
                           (temp_CRP['test_date']<=temp.loc[j,'start_datetime']+datetime.timedelta(days=15))]
        CRP_14d_value=df_CRP_14d['test_result'].astype('float').mean()
        temp.loc[j,'CRP_14d']=round(CRP_14d_value,2)
        # 提取CRP第14天检测与利伐沙班服药前CRP的差值
        CRP_14d_former_value=CRP_14d_value-CRP_former_value
        temp.loc[j, 'CRP_14d_former'] = round(CRP_14d_former_value, 2)
        # 提取CRP第28天检测
        df_CRP_28d=temp_CRP[(temp_CRP['test_date']>=temp.loc[j,'start_datetime'] + datetime.timedelta(days=27)) &
                           (temp_CRP['test_date']<=temp.loc[j,'start_datetime']+datetime.timedelta(days=29))]
        CRP_28d_value=df_CRP_28d['test_result'].astype('float').mean()
        temp.loc[j,'CRP_28d']=round(CRP_28d_value,2)
        # 提取CRP第28天检测与利伐沙班服药前CRP的差值
        CRP_28d_former_value=CRP_28d_value-CRP_former_value
        temp.loc[j, 'CRP_28d_former'] = round(CRP_28d_former_value, 2)

        # 提取利伐沙班服药前3天的WBC数值
        df_WBC_former=temp_WBC[(temp_WBC['test_date']<temp.loc[j,'start_datetime']) &
                             (temp_WBC['test_date'] >= temp.loc[j, 'start_datetime'] - datetime.timedelta(days=4))]
        print(df_WBC_former['test_result'])
        WBC_former_value=df_WBC_former['test_result'].astype('float').mean()
        temp.loc[j,'WBC_former']=round(WBC_former_value,2)
        # 提取WBC第1天检测
        df_WBC_1d = temp_WBC[(temp_WBC['test_date'] >= temp.loc[j, 'start_datetime']) &
                             (temp_WBC['test_date'] <= temp.loc[j, 'start_datetime'] + datetime.timedelta(days=1))]
        WBC_1d_value = df_WBC_1d['test_result'].astype('float').mean()
        temp.loc[j, 'WBC_1d'] =round(WBC_1d_value,2)
        # 提取WBC第1天检测与利伐沙班服药前WBC的差值
        WBC_1d_former_value=WBC_1d_value-WBC_former_value
        temp.loc[j, 'WBC_1d_former'] = round(WBC_1d_former_value, 2)
        # 提取WBC第3天检测
        df_WBC_3d=temp_WBC[(temp_WBC['test_date']>=temp.loc[j,'start_datetime'] + datetime.timedelta(days=2)) &
                           (temp_WBC['test_date']<=temp.loc[j,'start_datetime']+datetime.timedelta(days=4))]
        WBC_3d_value=df_WBC_3d['test_result'].astype('float').mean()
        temp.loc[j,'WBC_3d']=round(WBC_3d_value,2)
        # 提取WBC第3天检测与利伐沙班服药前WBC的差值
        WBC_3d_former_value=WBC_3d_value-WBC_former_value
        temp.loc[j, 'WBC_3d_former'] = round(WBC_3d_former_value, 2)
        # 提取WBC第7天检测
        df_WBC_7d=temp_WBC[(temp_WBC['test_date']>=temp.loc[j,'start_datetime'] + datetime.timedelta(days=6)) &
                           (temp_WBC['test_date']<=temp.loc[j,'start_datetime']+datetime.timedelta(days=8))]
        WBC_7d_value=df_WBC_7d['test_result'].astype('float').mean()
        temp.loc[j,'WBC_7d']=round(WBC_7d_value,2)
        # 提取WBC第7天检测与利伐沙班服药前WBC的差值
        WBC_7d_former_value=WBC_7d_value-WBC_former_value
        temp.loc[j, 'WBC_7d_former'] = round(WBC_7d_former_value, 2)
        # 提取WBC第14天检测
        df_WBC_14d=temp_WBC[(temp_WBC['test_date']>=temp.loc[j,'start_datetime'] + datetime.timedelta(days=13)) &
                           (temp_WBC['test_date']<=temp.loc[j,'start_datetime']+datetime.timedelta(days=15))]
        WBC_14d_value=df_WBC_14d['test_result'].astype('float').mean()
        temp.loc[j,'WBC_14d']=round(WBC_14d_value,2)
        # 提取WBC第14天检测与利伐沙班服药前WBC的差值
        WBC_14d_former_value=WBC_14d_value-WBC_former_value
        temp.loc[j, 'WBC_14d_former'] = round(WBC_14d_former_value, 2)
        # 提取WBC第28天检测
        df_WBC_28d=temp_WBC[(temp_WBC['test_date']>=temp.loc[j,'start_datetime'] + datetime.timedelta(days=27)) &
                           (temp_WBC['test_date']<=temp.loc[j,'start_datetime']+datetime.timedelta(days=29))]
        WBC_28d_value=df_WBC_28d['test_result'].astype('float').mean()
        temp.loc[j,'WBC_28d']=round(WBC_28d_value,2)
        # 提取WBC第28天检测与利伐沙班服药前WBC的差值
        WBC_28d_former_value=WBC_28d_value-WBC_former_value
        temp.loc[j, 'WBC_28d_former'] = round(WBC_28d_former_value, 2)
    temp_list.append(temp)
df_lfsb_other_test=temp_list[0]
for j in range(1,len(temp_list)):
    df_lfsb_other_test=pd.concat([df_lfsb_other_test,temp_list[j]],axis=0)
df_lfsb_other_test=df_lfsb_other_test.sort_values(['patient_id','start_datetime'])
df_lfsb_other_test=df_lfsb_other_test.reset_index(drop=True)

writer=pd.ExcelWriter(project_path+'/data/processed_data/df_3_其他检验信息.xlsx')
df_lfsb_other_test.to_excel(writer)
writer.save()










