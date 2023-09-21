import sys
import pandas as pd
import pybedtools

def sum_merged_csv_file(file):
    df = pd.read_csv(file)
    rows_exclude_sum = ['ref_file', 'chromossome', 'start','end']
    df['QtdPeaks'] = df.drop(rows_exclude_sum, axis=1).sum(axis=1)
    rows_keep = ['ref_file', 'chromossome', 'start','end','QtdPeaks']
    df_result = df.drop(columns=[coluna for coluna in df.columns if coluna not in rows_keep])
    df_result.to_csv('result_summed.csv', index=False)
    
    dfBed = df_result.loc[df_result['QtdPeaks'] <= 3]
    dfBed = dfBed.drop(columns=['ref_file','QtdPeaks'])
    dfBed['name'] = 'feature' + dfBed.index.astype(str)
    BedObj = pybedtools.BedTool.from_dataframe(dfBed)
    BedObj.saveas('result_3.bed')

    dfBed = df_result.loc[df_result['QtdPeaks'] <= 2]
    dfBed = dfBed.drop(columns=['ref_file','QtdPeaks'])
    dfBed['name'] = 'feature' + dfBed.index.astype(str)
    BedObj = pybedtools.BedTool.from_dataframe(dfBed)
    BedObj.saveas('result_2.bed')

    dfBed = df_result.loc[df_result['QtdPeaks'] <= 1]
    dfBed = dfBed.drop(columns=['ref_file','QtdPeaks'])
    dfBed['name'] = 'feature' + dfBed.index.astype(str)
    BedObj = pybedtools.BedTool.from_dataframe(dfBed)
    BedObj.saveas('result_1.bed')

    dfBed = df_result.loc[df_result['QtdPeaks'] <= 0]
    dfBed = dfBed.drop(columns=['ref_file','QtdPeaks'])
    dfBed['name'] = 'feature' + dfBed.index.astype(str)
    BedObj = pybedtools.BedTool.from_dataframe(dfBed)
    BedObj.saveas('result_0.bed')

def main():
    file_result = sys.argv[1]
    sum_merged_csv_file(file_result)

if __name__ == "__main__":
    main()