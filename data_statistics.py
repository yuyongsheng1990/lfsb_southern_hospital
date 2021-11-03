# _*_ coding: utf-8 _*_
# @Time: 2021/11/2 0:47
# @Author: yuyongsheng
# @Software: PyCharm
# @Description: 南方医院利伐沙班(2)：数据统计分析。
import pandas as pd
pd.set_option('mode.chained_assignment', None)
import numpy as np
import os
project_path=os.getcwd()

# 1. 高低剂量组的住院时长、日均剂量显著性检验
df_lfsb=pd.read_excel(project_path+r'/data/processed_data/df_3_其他检验信息.xlsx')
if 'Unnamed: 0' in df_lfsb.columns:
    df_lfsb = df_lfsb.drop(['Unnamed: 0'], axis=1)
# 以住院记录case_no为筛选标准，只保留一条数据
df_lfsb=df_lfsb.drop_duplicates(['case_no'],keep='first')
# 划分高低剂量组
df_lfsb_high=df_lfsb[df_lfsb['住院期间日均剂量']>=20]
df_lfsb_low=df_lfsb[df_lfsb['住院期间日均剂量']<20]

# 高低剂量组显著性分析，高于median为1，低于median为0
df_lfsb['住院期间日均剂量分组']=df_lfsb['住院期间日均剂量'].apply(lambda x: 0 if x <20 else 1)
writer=pd.ExcelWriter(project_path+'/data/result/df_stat_1_住院期间日均高低剂量分组.xlsx')
df_lfsb.to_excel(writer)
writer.save()
'''
# 住院期间日均剂量统计中位数和均值
median_value=df_lfsb['住院期间日均剂量'].median()
mean_value=df_lfsb['住院期间日均剂量'].mean()
print('住院日均剂量中位数',median_value)
print('住院日均剂量平均数',mean_value)
# 统计patient_id总数和case_no总数
patient_num=df_lfsb['patient_id'].nunique()
case_num=df_lfsb['case_no'].nunique()
print(patient_num)
print(case_num)
# 统计高低剂量组患者人数
high_num_patient=df_lfsb_high['patient_id'].nunique()
low_num_patient=df_lfsb_low['patient_id'].nunique()
# 统计住院case_no数目
high_num_case=df_lfsb_high['case_no'].nunique()
low_num_case=df_lfsb_low['case_no'].nunique()
print('统计高剂量组patient_id:',high_num_patient,high_num_patient/patient_num)
print('统计高剂量组case_no:',high_num_case,high_num_case/case_num)
print('统计低剂量组patient_id:',low_num_patient,low_num_patient/patient_num)
print('统计低剂量组case_no:', low_num_case,low_num_case/case_num)
# 统计高低剂量组住院时长：中位数和均值
print('------------------高低剂量组住院时长统计-----------------------')
# all_median_inp=df_lfsb['住院时长'].median()
# all_mean_inp=df_lfsb['住院时长'].mean()
# print('住院总时长中位数',all_median_inp)
# print('住院总时长平均数',all_mean_inp)
# 高剂量组住院时长统计
high_median_inp=df_lfsb_high['住院时长'].median()
high_mean_inp=df_lfsb_high['住院时长'].mean()
print('高剂量组住院时长中位数和平均数',high_median_inp,high_mean_inp)
# 低剂量组住院时长统计
low_median_inp=df_lfsb_low['住院时长'].median()
low_mean_inp=df_lfsb_low['住院时长'].mean()
print('低剂量组住院时长中位数和平均数',low_median_inp,low_mean_inp)
print('------------------高低剂量组用药时长统计-----------------------')
# all_median_inp=df_lfsb['住院期间服药时长'].median()
# all_mean_inp=df_lfsb['住院期间服药时长'].mean()
# print('住院期间总服药时长中位数',all_median_inp)
# print('住院期间总服药时长平均数',all_mean_inp)
# 高剂量组服药时长统计
high_dosage_median=df_lfsb_high['住院期间服药时长'].median()
high_dosage_mean=df_lfsb_high['住院期间服药时长'].mean()
print('高剂量组服药时长中位数和平均数',high_dosage_median,high_dosage_mean)
# 低剂量组服药时长统计
low_dosage_median=df_lfsb_low['住院期间服药时长'].median()
low_dosage_mean=df_lfsb_low['住院期间服药时长'].mean()
print('低剂量组服药时长中位数和平均数',low_dosage_median,low_dosage_mean)
'''

# 高低剂量组利伐沙班服药前后WBC显著性检验
from scipy.stats import kstest,shapiro
import scipy.stats as st
from scipy.stats import chi2_contingency
##检验是否正态
def norm_test(data):
    if len(data) > 30:
        norm, p = kstest(data, 'norm')
    else:
        norm, p = shapiro(data)
    #print(t,p)
    if p>=0.05:
        return True
    else:
        return False

def test2(data_b, data_p):
    if norm_test(data_b) and norm_test(data_p):
        x = 1
        y = '独立样本T检验'
        t, p = st.ttest_ind(list(data_b),list(data_p), nan_policy='omit')
    else:
        x = 0
        y = 'Mann-Whitney U检验'
        t,p = st.mannwhitneyu(list(data_b),list(data_p))
    return x,y,t,p
def sig_test(df_high,df_low,list):

    field_list=[]
    y_list=[]
    t_list=[]
    p_list=[]
    result_list=[]
    high_mean_list=[]
    low_mean_list=[]
    # high_num_list=[]
    # high_rate_list=[]
    # low_num_list=[]
    # low_rate_list=[]
    for i in range(len(list)):
        field=list[i]
        df_high_nt=df_high[df_high[field].notnull()]
        data_high=df_high_nt[field]
        high_mean=round(data_high.mean(),2)
        # high_num=df_high_nt.shape[0]
        # all_num=df_high.shape[0] + df_low.shape[0]
        # high_rate = "%.2f%%" % (round(high_num/all_num) * 100)
        df_low_nt=df_low[df_low[field].notnull()]
        data_low=df_low_nt[field]
        low_mean=round(data_low.mean(),2)
        # low_num=df_low_nt.shape[0]
        # low_rate="%.2f%%" % (round(low_num/all_num) * 100)
        if data_high.shape[0] >= 10 and data_low.shape[0]>=10:
            x,y,t,p = test2(data_high, data_low)
            if p <=0.05:
                sig='显著'
            else:
                sig='不显著'
            field_list.append(field)
            y_list.append(y)
            t_list.append(t)
            p_list.append(p)
            result_list.append(sig)
            high_mean_list.append(high_mean)
            low_mean_list.append(low_mean)
            # high_num_list.append(high_num)
            # high_rate_list.append(high_rate)
            # low_num_list.append(low_num)
            # low_rate_list.append(low_rate)
    df_result=pd.DataFrame({'特征':field_list,
                            '高剂量均值':high_mean_list,
                            '低剂量均值':low_mean_list,
                            # '高剂量数目':high_num_list,
                            # '高剂量比例':high_rate_list,
                            # '低剂量数目':low_num_list,
                            # '低剂量比例':low_rate_list,
                            '检验指标':y_list,
                            't值':t_list,
                            'p值':p_list,
                            '显著性结果':result_list})
    return df_result
# 住院时长到用药时长的显著性检验
df_inp_time=sig_test(df_lfsb_high,df_lfsb_low,['住院时长'])
df_inp_time=df_inp_time.reset_index(drop=True)

writer=pd.ExcelWriter(project_path+r'/data/result/df_高低剂量组住院时长显著性检验.xlsx')
df_inp_time.to_excel(writer)
writer.save()

# 高低剂量组WBC和CRP显著性检验
WBC_list=['WBC_former','WBC_1d','WBC_3d','WBC_7d','WBC_14d','WBC_28d']
WBC_diff_list=['WBC_1d_former','WBC_3d_former','WBC_7d_former','WBC_14d_former','WBC_28d_former']
CRP_list=['CRP_former','CRP_1d','CRP_3d','CRP_7d','CRP_14d','CRP_28d']
CRP_diff_list=['CRP_1d_former','CRP_3d_former','CRP_7d_former','CRP_14d_former','CRP_28d_former']
feature_list=[WBC_list,WBC_diff_list,CRP_list,CRP_diff_list]
name_list=['WBC','WBC_diff','CRP','CRP_diff']
temp_list=[]
for i,j in zip(feature_list,name_list):
    temp=sig_test(df_lfsb_high,df_lfsb_low,i)
    temp=temp.reset_index(drop=True)
    temp_list.append(temp)
df_result=temp_list[0]
for j in range(1,len(temp_list)):
    df_result=pd.concat([df_result,temp_list[j]],axis=0)

writer=pd.ExcelWriter(project_path+r'/data/result/df_高低剂量组WBC和CRP显著性检验.xlsx')
df_result.to_excel(writer)
writer.save()


# 高低剂量组入院到出院时间均值和显著性检验
inp_oup_list=['第1次出院_第2次入院','第2次出院_第3次入院','第3次出院_第4次入院','第4次出院_第5次入院','第5次出院_第6次入院','第6次出院_第7次入院','第7次出院_第8次入院','第8次出院_第9次入院']
df_inp_oup=sig_test(df_lfsb_high,df_lfsb_low,inp_oup_list)
df_inp_oup=df_inp_oup.reset_index(drop=True)

writer=pd.ExcelWriter(project_path+r'/data/result/df_高低剂量组入院到出院显著性检验.xlsx')
df_inp_oup.to_excel(writer)
writer.save()

