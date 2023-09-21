import sys
import pybedtools
import pandas as pd
import os

def main():
    if len(sys.argv) != 3:
        print("Error: This programm needs two parameters to work.")
        sys.exit(1)

    par1 = sys.argv[1]
    par2 = sys.argv[2]

    print("Arquivo Ref:", par1)
    print("Arquivo Encode:", par2)
    count_hits(par2,par1)

def count_hits(fileEncode, fileRef):
    print('fileEncode:',fileEncode)
    print('fileRef:',fileRef)
    columns=['ref_file', 'chromossome', 'start','end','file','qtd_peaks']
    df = pd.DataFrame(columns=columns)
    
    ref_bed_encode = pybedtools.BedTool(fileEncode)
    ref_bed = pybedtools.BedTool(fileRef)
    
    ## convert to an interval object, to uso the count_hits function
    ref_bed_intervals = ref_bed_encode.as_intervalfile()
    valores = [[]]
    
    for ref_line in ref_bed:
        ## Count how many hits the interval from the current bed file interval has on this bedfile being analysed
        qtd = ref_bed_intervals.count_hits(ref_line)
        
        ## if the name of the is passed with the path returns just the filename
        name_file_encode = fileEncode[fileEncode.rfind('/')+1:]
        ref_file = fileRef[fileRef.rfind('/')+1:]
        
        valores = [ref_file,ref_line[0],ref_line[1],ref_line[2],name_file_encode,int(qtd)]
        new_df = pd.DataFrame([valores], columns=columns)
        df = pd.concat([df, new_df], ignore_index=True)
   
    colunas = ['ref_file','chromossome']
    pivot_df = df.pivot(index=['ref_file', 'chromossome', 'start', 'end'],
                    columns='file',
                    values='qtd_peaks').reset_index()
    
    ## exclude the extensio bed to use in the file name
    name_tissue = name_file_encode.replace('.bed',"")
    pivot_df.to_csv(name_tissue + '_result1.csv', index=False)

if __name__ == "__main__":
    main()