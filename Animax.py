import pandas as pd
import numpy as np
import datetime
import math
import csv
import re
import calendar
from collections import OrderedDict
import dc.utils
import dc.io
import dc.header
import dc.mapping
import dc.settings

def convert_linear(in_io, out_io, out_format="preV1506", owner_channel="Animax"):
    "Convert linear schedule for Animax"

    # Read input file
    indf = pd.read_excel(in_io, sheetname=0, na_values=["n/a"], keep_default_na=True, skiprows=0)
    indf.columns = ["".join(s.split()).replace("(", "").replace(")", "").replace("/", "").lower() for s in indf.columns]

    
    # Create output DataFrame
    outdf = pd.DataFrame(columns=dc.header.internalHeader)

    # Extract data from full title


    # Fill output DataFrame with related info
    indf['fulltitleenglishmax40characters'] = ''
    indf['fulltitlechinesemax24characters'] = ''
    excepted_column = ["PID", "Premier", "Recordable", "Is NPVR Prog", "Is Restart TV", "effective_date", "expiration_date", "Media ID (max. 12 chars)"]
    for internal_column, indf_column in dc.header.postV1506HeaderMapping.items():
        if (indf_column not in excepted_column): 
            edited_indf_column = "".join(indf_column.split()).replace("(", "").replace(")", "").replace("/", "").lower()
            if (edited_indf_column in indf.columns):
                outdf[internal_column] = indf[edited_indf_column]
                if (outdf[internal_column].dtype == object):
                    outdf[internal_column] = outdf[internal_column].where(outdf[internal_column].apply(lambda x: pd.notnull(x) and len(str(x).strip()) > 0))
    outdf['ChannelNo'] = 150
    outdf["TXDate"] = outdf["TXDate"].apply(lambda x: pd.to_datetime(str(x), format="%Y%m%d")).apply(lambda x: x.strftime("%Y%m%d"))
    outdf["ActualTime"] = outdf["ActualTime"].apply(lambda x: x.strftime("%H:%M"))
    from datetime import timedelta   
    txDate = outdf[['TXDate', 'ActualTime']].apply(lambda x: ' '.join(x), axis=1).apply(lambda x: pd.to_datetime(str(x), format="%Y%m%d %H:%M"))
    txDate = txDate.apply(lambda x: x + timedelta(days=1) if x.hour < 6 else x).apply(lambda x: x.strftime("%Y%m%d"))
    outdf["TXDate"] = txDate
    fullTitleEng = indf["brandnameenglish"][indf["brandnameenglish"].notnull()].apply(lambda x: str(x).strip())
    fullTitleChi = indf["brandnamechinese"][indf["brandnamechinese"].notnull()].apply(lambda x: str(x).strip())
    extractedInfo = pd.DataFrame(columns=["BrandNameEng", "BrandNameChi", "SeasonNo", "EpisodeNo", "EpisodeNameChi", "EpisodeNameEng"])
    tempExtractedInfo = [
        fullTitleChi.str.extract("(?P<BrandNameChi>.*?)\s?:?[\(（]+第(?P<EpisodeNo>[0-9一二三四五六七八九十百千萬]+)[季期]+[\)）]+$", expand=False)
        ,fullTitleChi.str.extract("(?P<BrandNameChi>.*?)\s(?P<EpisodeNo>\d)$", expand=False)
        ,fullTitleChi.str.extract("(?P<BrandNameChi>.*?)\s(?P<EpisodeNo>(?=[MDCLXVI])M*(C[MD]|D?C{0,3})(X[CL]|L?X{0,3})(I[XV]|V?I{0,3}))$", expand=False)
        ,fullTitleChi.str.extract("(?P<BrandNameChi>.*?)\s\(.*?(?P<EpisodeNo>\d+).*?\)$", expand=False)
        ,fullTitleChi.str.extract("(?P<BrandNameChi>.*)", expand=False)
        ,fullTitleEng.str.extract("(?P<BrandNameEng>.*?)\s\([Ss][Ee][Aa][Ss][Oo][Nn]\s+(?P<SeasonNo>\d+)\)$", expand=False)
        ,fullTitleEng.str.extract("(?P<BrandNameEng>.*?)\s\(.*?(?P<EpisodeNo>\d+).*?\)$", expand=False)
        ,fullTitleEng.str.extract("(?P<BrandNameEng>.*?)\s(?P<EpisodeNo>\d)$", expand=False)
        ,fullTitleEng.str.extract("(?P<BrandNameEng>.*?)\s(?P<EpisodeNo>(?=[MDCLXVI])M*(C[MD]|D?C{0,3})(X[CL]|L?X{0,3})(I[XV]|V?I{0,3}))$", expand=False)
        ,fullTitleEng.str.extract("(?P<BrandNameEng>.*)", expand=False)
    ]
    extractedInfo["BrandNameChi"]= tempExtractedInfo[0]["BrandNameChi"]
    extractedInfo["BrandNameChi"].fillna(tempExtractedInfo[1]["BrandNameChi"], inplace=True)
    extractedInfo["BrandNameChi"].fillna(tempExtractedInfo[2]["BrandNameChi"], inplace=True)
    extractedInfo["BrandNameChi"].fillna(tempExtractedInfo[4], inplace=True)
    extractedInfo["BrandNameEng"]= tempExtractedInfo[8]["BrandNameEng"]
    extractedInfo["BrandNameEng"].fillna(tempExtractedInfo[5]["BrandNameEng"], inplace=True)
    extractedInfo["BrandNameEng"].fillna(tempExtractedInfo[7]["BrandNameEng"], inplace=True)
    extractedInfo["BrandNameEng"].fillna(tempExtractedInfo[9], inplace=True)
    outdf["EpisodeNameEng"] = outdf["EpisodeNameEng"].str.replace("EPISODE \d+$", "")
    outdf["EpisodeNameEng"].fillna("", inplace=True)
    outdf["SponsorTextStuntChi"].fillna(outdf["SponsorTextStuntEng"], inplace=True)
    outdf["SponsorTextStuntEng"].fillna(outdf["SponsorTextStuntChi"], inplace=True)
    outdf["BrandNameChi"] = extractedInfo["BrandNameChi"]
    outdf["BrandNameEng"] = extractedInfo["BrandNameEng"]
    outdf["BrandNameChi"].fillna(outdf["BrandNameEng"], inplace=True)
    outdf["BrandNameEng"].fillna(outdf["BrandNameChi"], inplace=True)
    outdf["SeasonNameChi"].fillna(outdf["SeasonNameEng"], inplace=True)
    outdf["SeasonNameEng"].fillna(outdf["SeasonNameChi"], inplace=True)
    outdf["EpisodeNameChi"].fillna(outdf["EpisodeNameEng"], inplace=True)
    outdf["EpisodeNameEng"].fillna(outdf["EpisodeNameChi"], inplace=True)
    outdf["SynopsisChi"].fillna(outdf["SynopsisEng"], inplace=True)
    outdf["SynopsisEng"].fillna(outdf["SynopsisChi"], inplace=True)
    outdf["ShortSynopsisChi"].fillna(outdf["ShortSynopsisEng"], inplace=True)
    outdf["ShortSynopsisEng"].fillna(outdf["ShortSynopsisChi"], inplace=True)
    outdf["SeasonNo"] = outdf["SeasonNo"][outdf["SeasonNo"].notnull()].astype(int).astype(str)
    outdf["EpisodeNo"] = outdf["EpisodeNo"][outdf["EpisodeNo"].notnull()].astype(int).astype(str)
    outdf["FirstReleaseYear"] = outdf["FirstReleaseYear"][outdf["FirstReleaseYear"].notnull()].astype(int).astype(str)
    outdf["IsEpisodic"] = (outdf["EpisodeNo"].notnull() | outdf["EpisodeNameEng"].notnull() | outdf["EpisodeNameChi"].notnull()).map({True:"Y", False:"N"})
    text_column = ["SponsorTextStuntEng", "SponsorTextStuntChi", "BrandNameEng", "BrandNameChi", "EditionVersionEng", "EditionVersionChi", "SeasonNameEng", "SeasonNameChi", "EpisodeNameEng", "EpisodeNameChi", "SynopsisEng", "SynopsisChi", "ShortSynopsisEng", "ShortSynopsisChi", "DirectorProducerEng", "DirectorProducerChi", "CastEng", "CastChi"]
    for column in text_column:
        outdf[column] = outdf[column].astype(str).where(outdf[column].notnull()).str.translate(dc.mapping.charTranslateTable)
    dc.utils.title_df(outdf)
    outdf[["FullTitleEng", "FullTitleChi"]] = outdf.apply(dc.utils.constructFullTitle, axis=1)
    outdf[["ProgrammeNameEng", "ProgrammeNameChi"]] = outdf.apply(dc.utils.constructProgrammeName, axis=1)
    dc.utils.translateChar(outdf)
    dc.utils.chopCharacters(outdf)
    outdf = outdf[outdf.FullTitleEng.str.lower() != 'end of file']


    # Append EOF
    dc.utils.appendEOF(outdf)


    # Fill Channel #
    outdf['OwnerChannel'] = owner_channel
    outdf['ChannelNo'] = outdf['OwnerChannel'].replace(dc.mapping.ownerChannelMapping)

    # Write output file
    dc.io.to_io(outdf, out_io, out_format)

def convert_nonlinear(in_io, out_io, format="preV1506", owner_channel="Animax"):
    "Convert nonlinear for Animax"
    return None

