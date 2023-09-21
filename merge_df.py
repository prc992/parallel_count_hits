import sys
import pandas as pd

def main():
    files = sys.argv[1:]
    merge_csv_files(files)

def merge_csv_files(file_list):
    merged_df = pd.read_csv(file_list[0])

    for file_path in file_list[1:]:
        
        cols = pd.read_csv(file_path, nrows=1).columns
        df = pd.read_csv(file_path,usecols=cols[len(cols)-1:len(cols)])

        column_name = df.columns[0]
        #df.rename(columns={column_name: f"{column_name}_alo"}, inplace=True)  # Renomeia a coluna
        merged_df = pd.merge(merged_df, df, left_index=True, right_index=True, how='inner')
    
    merged_df.to_csv('result1.csv', index=False)

if __name__ == "__main__":
    main()