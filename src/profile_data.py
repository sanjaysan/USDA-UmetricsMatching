import pandas as pd
import math
import numpy as np


def is_primarykey(df_column):
    # Checking if column has any missing values
    if not df_column.isnull().any() and df_column.count() == len(df_column.unique()):
        return True
    return False


def remove_nan_from_set(input_set):
    return [item for item in input_set if str(item) != 'nan']


def add_employee_full_name_to_umetrics(umetrics_df, umetrics_emp_df):
    merged_df = umertics_df.merge(umetrics_emp_df, how='inner', on='UniqueAwardNumber')
    merged_df = merged_df.drop('Unnamed: 0', axis=1)
    unique_award_no_df = merged_df.groupby('UniqueAwardNumber')['FullName'].apply(set)
    unique_award_no_df = unique_award_no_df.reset_index()
    result_df = umetrics_df.merge(unique_award_no_df, how='inner', on='UniqueAwardNumber')
    result_df = result_df.rename(index=str, columns={"FullName": "EmployeeName"})
    result_df['EmployeeName'] = result_df['EmployeeName'].apply(remove_nan_from_set)
    result_df['EmployeeName'] = result_df['EmployeeName'].apply(lambda x: " | ".join(x) if x else np.NaN)
    return result_df


def normalize_attribute_names(usda_csv):
    usda_df = pd.read_csv(usda_csv)
    usda_df = usda_df.rename(index=str, columns={"Project Title": "AwardTitle",
                                                 "Award Number": "UniqueAwardNumber",
                                                 "Project Start Date": "FirstTransDate",
                                                 "Project End Date": "LastTransDate",
                                                 "Project Director": "EmployeeName"})
    return usda_df


if __name__ == '__main__':
    umetrics_csv = '../Dataset/UMETRICS_Award_Agg_Matching.csv'
    umetrics_emp_csv = '../Dataset/UMETRICS_Employees_Matching.csv'
    usda_csv = '../Dataset/USDA_Award_Matching.csv'

    pd.set_option('display.expand_frame_repr', False)

    umertics_df = pd.read_csv(umetrics_csv)
    usda_df = pd.read_csv(usda_csv)

    umetric_primary_key = is_primarykey(umertics_df['UniqueAwardNumber'])
    print umetric_primary_key
    usda_primary_key = is_primarykey(usda_df['Accession Number'])
    print usda_primary_key

    umetrics_emp_df = pd.read_csv(umetrics_emp_csv)

    umetrics_with_emp_df = add_employee_full_name_to_umetrics(umertics_df, umetrics_emp_df)
    usda_df = normalize_attribute_names(usda_csv)

    usda_df.to_csv('../Dataset/USDA_Award_Matching_copy.csv')
    umetrics_with_emp_df.to_csv('../Dataset/UMETRICS_Award_Agg_Matching_copy.csv')
