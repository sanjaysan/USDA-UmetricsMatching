import py_entitymatching as em
import pandas as pd


def is_primarykey(df_column):
    # Checking if column has any missing values
    if not df_column.isnull().any() and df_column.count() == len(df_column.unique()):
        return True
    return False

def add_employee_full_name_to_umetrics(umetrics_df, usda_df, umetrics_emp_df):

    print usda_df[usda_df['UniqueAwardNumber'].isin(umetrics_emp_df['UniqueAwardNumber'])]

def normalize_attribute_names(umetrics_csv, usda_csv):
    #
    umetrics_df = em.read_csv_metadata(umetrics_csv, key='UniqueAwardNumber')
    usda_df = em.read_csv_metadata(usda_csv, key='Accession Number')
    umetrics_cols = umetrics_df.columns
    usda_cols = usda_df.columns
    #
    # print(len(umetrics_cols))
    # print (len(usda_cols))

    df1 = pd.DataFrame({'UMetric_cols':umetrics_cols})
    df1.to_csv('df1.csv')
    df2 = pd.DataFrame({'USDA_cols':usda_cols})
    df2.to_csv('df2.csv')



    id = range(max(len(df1), len(df2)))
    # col_df = pd.DataFrame(columns=['UMetric_cols','USDA_cols'], index=id)
    col_df = pd.concat([df1, df2], axis = 1)
    col_df.insert(0, 'id', id)
    # print col_df
    col_df.to_csv('col_df.csv')
    # #
    # #
    # # A = em.read_csv_metadata('df1.csv', key='UMetric_cols')
    # # B = em.read_csv_metadata('df2.csv', key='USDA_cols')
    # # col_df_em = em.read_csv_metadata('col_df.csv', key='id')
    # # # print type(umetrics_cols)
    # # ob = em.OverlapBlocker()
    # # C1 = ob.block_tables(A, B, 'UMetric_cols', 'USDA_cols', word_level=True, overlap_size=3,
    # #                      l_output_attrs=['UMetric_cols'],
    # #                      r_output_attrs=['USDA_cols'],
    # # show_progress = False)
    # # print C1.head()

    usda_df = pd.read_csv(usda_csv)
    usda_df = usda_df.rename(index=str, columns={"Project Title":"AwardTitle",
                                                 "Award Number":"UniqueAwardNumber",
                                                 "Project Start Date":"FirstTransDate",
                                                 "Project End Date":"LastTransDate"})
    usda_df.to_csv('USDA_Award_Matching_copy.csv')
    return usda_df




if __name__ == '__main__':

    umetrics_csv = '../Dataset/UMETRICS_Award_Agg_Matching.csv'
    umetrics_emp_csv = '../Dataset/UMETRICS_Employees_Matching.csv'
    usda_csv = '../Dataset/USDA_Award_Matching.csv'
    pd.set_option('display.expand_frame_repr', False)

    umertics_df = pd.read_csv(umetrics_csv)
    umetric_primary_key = is_primarykey(umertics_df['UniqueAwardNumber'])
    # print umetric_primary_key

    usda_df = pd.read_csv(usda_csv)
    usda_primary_key = is_primarykey(usda_df['Accession Number'])
    # print usda_primary_key

    usda_df = normalize_attribute_names(umetrics_csv, usda_csv)

    umetrics_emp_df = pd.read_csv(umetrics_emp_csv)
    add_employee_full_name_to_umetrics(umertics_df, usda_df, umetrics_emp_df)
