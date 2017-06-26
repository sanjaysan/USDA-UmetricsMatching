import pandas as pd
import os

def describe_col(A, col):
    values = A[col]
    d = {}
    d['column'] = col
    d['count'] = len(values)

    values_nona = values.dropna()
    d['missing'] = d['count'] - len(values_nona)

    if values_nona.dtype != 'object':
        d['type'] = 'numeric'
        d['max'] = values_nona.max()
        d['min'] = values_nona.min()
        d['median'] = values_nona.median()
        d['mean'] = values_nona.mean()
        d['unique'] = len(pd.unique(values_nona))
    else:
        values_nona = values_nona.astype(str)
        d['unique'] = len(pd.unique(values_nona.str.lower()))
        temp = values_nona.apply(len)
        d['type'] = 'string'
        d['max'] = temp.max()
        d['min'] = temp.min()
        d['median'] = temp.median()
        d['mean'] = temp.mean()
    d['missing_perc'] = format(d['missing'] * 100.0 / float(d['count']), '.4f')
    return d


def frame_metadata(df):
    dict_list_for_each_column = []
    for column in df.columns:
        dict_list_for_each_column.append(describe_col(df, column))
    pd.set_option('display.expand_frame_repr', False)
    metadata = pd.DataFrame(dict_list_for_each_column)
    new_metadata = metadata.set_index('column')
    print new_metadata


def get_data_files(directory):
    csv_files = [filename for filename in os.listdir(directory) if filename.endswith('.csv')]
    return csv_files


if __name__ == '__main__':

    directory = '../Dataset'
    csv_files = get_data_files(directory)

    samples = []
    for csv_file in csv_files:
        df = pd.read_csv(os.path.join(directory, csv_file))
        frame_metadata(df)
        samples.append(df.sample(20))
