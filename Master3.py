# coding: utf-8

from multiprocessing import Process,Pool,cpu_count, active_children, Manager
import time
import pandas as pd
#import Levenshtein
#import dc
import dc.header
#import dc.utils
#import dc.mapping
#import re
#from fuzzywuzzy import process
from fuzzywuzzy import fuzz
#import numpy as np
import heapq

in_io = "input/Title Matching Algorithm_Sample_Jun-Aug 2018 (20180907).xls"
    
indf = pd.read_excel(in_io, sheet_name=0, keep_default_na=False, na_values=[''])
indf.dropna(how='all', inplace=True)
k = 3
columns = [
        "ProductID_DB", 
        "Score", 
        "AllClassificationMatch", 
        "FullTitleEng_DB", 
        "FullTitleChi_DB",
        'Match_Eng',
        'Match_Chi',
        "LibraryEng", 
        "LibraryChi", 
        #"FullTitleEngOld", 
        #"FullTitleChiOld", 
        "BrandNameEng", 
        "BrandNameChi", 
        "EditionVersionEng", 
        "EditionVersionChi", 
        "SeasonNo", 
        "SeasonNameEng", 
        "SeasonNameChi", 
        "EpisodeNo", 
        "EpisodeNameEng", 
        "EpisodeNameChi", 
        "SponsorTextStuntEng", 
        "SponsorTextStuntChi", 
        "FirstReleaseYear", 
        "DirectorProducerEng",
        "DirectorProducerChi",
        "CastEng",
        "CastChi",
        "OriginalLang",
        "RegionCode",
        "Genre",
        "SubGenre",
]
out_columns = []
for  i in range(k):
    out_columns.extend([
        "EN Result " + str(i+1) + " Total Score",
        "result_"+str(i+1)+"_en_product ID_DB",
        "result_"+str(i+1)+"_BrandNameEng",
        "result_"+str(i+1)+"_en_SeasonNo",
        "result_"+str(i+1)+"_SeasonNameEng",
        "result_"+str(i+1)+"_en_EpisodeNo",
        "result_"+str(i+1)+"_EpisodeNameEng",
        "result_"+str(i+1)+"_SYNOPSIS_ENG",
        "result_"+str(i+1)+"_GENRE_ENG",
        "CH Result " + str(i+1) + " Total Score",
        "result_"+str(i+1)+"_ch_product ID_DB",
        "result_"+str(i+1)+"_BrandNameChi",
        "result_"+str(i+1)+"_ch_SeasonNo",
        "result_"+str(i+1)+"_SeasonNameChi",
        "result_"+str(i+1)+"_ch_EpisodeNo",
        "result_"+str(i+1)+"_EpisodeNameChi",
        "result_"+str(i+1)+"_SYNOPSIS_CHI",
        "result_"+str(i+1)+"_GENRE_CHI"
	])
outdf = pd.DataFrame(columns=columns)
indf_columns1 = ["".join(s.split()).replace("#", "").replace(".", "").replace("(", "").replace(")", "").replace("/", "").lower() for s in indf.columns]
for internal_column, indf_column in dc.header.nlPostV1506HeaderMapping.items():
    edited_indf_column = "".join(indf_column.split()).replace("#", "").replace(".", "").replace("(", "").replace(")", "").replace("/", "").lower() #indf_column
    #print(edited_indf_column)
    if (edited_indf_column in indf_columns1):
        #print(edited_indf_column)
        outdf[internal_column] = indf[indf.columns[indf_columns1.index(edited_indf_column)]]
        if (outdf[internal_column].dtype == object):
            outdf[internal_column] = outdf[internal_column].where(outdf[internal_column].apply(lambda x: pd.notnull(x) and len(str(x).strip()) > 0))
#vod_library = pd.read_pickle("dc/data/vod_db.p")
vod_library = pd.read_excel("dc/data/vod_db_data.xlsx", sheet_name=0)
#result_en = []
#result_ch = []
result_all = []
def calculate_score(row):
    index, record = row
    compare_map = {
                    'BRAND_NAME_ENG': 'BrandNameEng',
                    'SEASON_NUM': 'SeasonNo',
                    'SEASON_NAME_ENG': 'SeasonNameEng',
                    'EPISODE_NUM': 'EpisodeNo',
                    'EPISODE_NAME_U3_ENG': 'EpisodeNameEng',
                    #'PRODUCT_VERSION_ENG': 'EditionVersionEng',
                  }
    compare_map1 = {
                    'BRAND_NAME_CHI': 'BrandNameChi',
                    'SEASON_NUM': 'SeasonNo',
                    'SEASON_NAME_CHI': 'SeasonNameChi',
                    'EPISODE_NUM': 'EpisodeNo',
                    'EPISODE_NAME_U3_CHI': 'EpisodeNameChi',
                    #'PRODUCT_VERSION_CHI': 'EditionVersionChi',
                  }

    vod_library_score = vod_library.apply(lambda x: sum([fuzz.WRatio(str(x[key]), str(record[value])) for key, value in compare_map.items()]), axis=1)

    pid_score_bests = [(i, j) for i, j in zip(vod_library['PID'], vod_library_score)]
    #result = heapq.nlargest(k, pid_score_bests, key=lambda x: x[1])
    result = sorted(pid_score_bests, key=lambda i: i[1], reverse=True)[:3]

    vod_library_score1 = vod_library.apply(lambda x: sum([fuzz.WRatio(str(x[key]), str(record[value])) for key, value in compare_map1.items()]), axis=1)

    pid_score_bests1 = [(i, j) for i, j in zip(vod_library['PID'], vod_library_score1)]
    #result1 = heapq.nlargest(k, pid_score_bests1, key=lambda x: x[1])
    result1 = sorted(pid_score_bests1, key=lambda i: i[1], reverse=True)[:3]
	
    return (result, result1)

#def calcuresult(row):
#    index, data = row
#    result, result1 = calculate_score(data, k)
    #calculate_score(data, k, "E")
    #calculate_score(data, k, "C")
	
def constructScore(x, flag):
	if flag == "E":
		tmp = [x[i][1]]
		tmp.extend(vod_library[[
                 "PID", 
                 "BRAND_NAME_ENG", 
                 "SEASON_NUM", 
                 "SEASON_NAME_ENG", 
                 "EPISODE_NUM", 
                 "EPISODE_NAME_U3_ENG",
                 "SYNOPSIS_ENG",
                 "GENRE",
    	]].loc[vod_library['PID'].astype('str') == str(x[i][0])].iloc[0].tolist())
		return pd.Series(tmp)
	else:
		tmp = [x[i][1]]
		tmp.extend(vod_library[[
                 "PID", 
                 "BRAND_NAME_CHI", 
                 "SEASON_NUM", 
                 "SEASON_NAME_CHI", 
                 "EPISODE_NUM", 
                 "EPISODE_NAME_U3_CHI",
                 "SYNOPSIS_CHI",
                 "GENRE",
    	]].loc[vod_library['PID'].astype('str') == str(x[i][0])].iloc[0].tolist())
		return pd.Series(tmp)
		

if __name__ == '__main__':
    pool = Pool(cpu_count()) 	
    result_all = pool.map(calculate_score, [(i,j) for i,j in outdf.iterrows()])   

    start = time.time()
    #pool.start()
    #print(str(result_all))
	#for it in result_all:
	#	result_en = result_en.append(it[0])
	#	result_ch = result_en.append(it[1])
    outdf['Match_Eng'] = pd.Series([en for en, ch in iter(result_all)])
    outdf['Match_Chi'] = pd.Series([ch for en, ch in iter(result_all)])

    print("The number of CPU is:" + str(cpu_count()))
    for p in active_children():
        print("child p.name: " + p.name + "\tp.id: " + str(p.pid))

    pool.close()
    pool.join()


    end = time.time()
    print('3 processes take %s seconds' % (end - start))

    for i in range(k):
    # eng
    	outdf[[
           "EN Result " + str(i+1) + " Total Score",
           "result_"+str(i+1)+"_en_product ID_DB", 
           "result_"+str(i+1)+"_BrandNameEng", 
           "result_"+str(i+1)+"_en_SeasonNo", 
           "result_"+str(i+1)+"_SeasonNameEng", 
           "result_"+str(i+1)+"_en_EpisodeNo", 
           "result_"+str(i+1)+"_EpisodeNameEng",
           "result_"+str(i+1)+"_SYNOPSIS_ENG",
           "result_"+str(i+1)+"_GENRE_ENG"
        ]] = outdf['Match_Eng'].apply(constructScore, args=("E",)
        )
    
    #chi
    	outdf[[
           "CH Result " + str(i+1) + " Total Score",
           "result_"+str(i+1)+"_ch_product ID_DB", 
           "result_"+str(i+1)+"_BrandNameChi", 
           "result_"+str(i+1)+"_ch_SeasonNo", 
           "result_"+str(i+1)+"_SeasonNameChi", 
           "result_"+str(i+1)+"_ch_EpisodeNo", 
           "result_"+str(i+1)+"_EpisodeNameChi",
           "result_"+str(i+1)+"_SYNOPSIS_CHI",
           "result_"+str(i+1)+"_GENRE_CHI"
        ]] = outdf['Match_Chi'].apply(constructScore, args=("C",)
        )
    outdf.drop(['Match_Eng', 'Match_Chi'], axis=1, inplace=True)
    output_columns = ["ProductID_DB", 'Score']
    output_columns.extend(out_columns)
    outdf1 = outdf[output_columns].join(indf)
    out_io = 'output/POC_test_v2.xlsx'
    writer = pd.ExcelWriter(out_io, engine='xlsxwriter')
    outdf1.to_excel(writer, sheet_name='input_vod_match', index=False)                      
    writer.save()
    exit(0)