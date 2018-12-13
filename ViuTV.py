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
import dc.APIMapping
import dc.APIWrapper

def convert_linear(in_io, out_io, out_format="preV1506", owner_channel="ViuTV"):
    "Convert linear schedule for ViuTV"

    # Read input file
    indf = pd.read_json(in_io)
    indf.dropna(how="any", subset=["txDate"], inplace=True)

    
    # Create output DataFrame
    outdf = pd.DataFrame(columns=dc.header.internalHeader)

    # Extract data from full title


    # Fill output DataFrame with related info
    indf = indf[pd.notnull(indf["txDate"])]
    cnt = 0
    for i in indf["txDate"]:
        try:
            test = pd.to_datetime(str(i), format="%Y-%m-%d %H:%M:%S")
        except ValueError:
            raise ValueError("Planning Date line "+str(cnt)+" occured errors")
        finally:
            cnt += 1
    outdf['TXDate'] = indf["txDate"].apply(lambda x: pd.to_datetime(str(x), format="%Y-%m-%d %H:%M:%S"))
    outdf["ActualTime"] = outdf["TXDate"].astype('str').str.slice(-8).str.slice(0, 5).str.zfill(5)
    outdf['TXDate'] = outdf['TXDate'].apply(lambda x: x.strftime("%Y%m%d"))
    outdf['SponsorTextStuntChi'] = indf['sponsorTextStuntChi'].replace('^-$',np.nan, regex=True)
    outdf['SponsorTextStuntEng'] = indf['sponsorTextStuntEng'].replace('^-$',np.nan, regex=True)
    outdf['BrandNameChi'] = indf['brandNameChi'].replace('^-$',np.nan, regex=True)
    outdf['BrandNameEng'] = indf['brandNameEng'].replace('^-$',np.nan, regex=True)
    outdf['EditionVersionChi'] = indf['editionVersionChi'].replace('^-$',np.nan, regex=True)
    outdf['EditionVersionEng'] = indf['editionVersionEng'].replace('^-$',np.nan, regex=True)
    outdf['SeasonNameChi'] = indf['seasonNameChi'].replace('^-$',np.nan, regex=True)
    outdf['SeasonNameEng'] = indf['seasonNameEng'].replace('^-$',np.nan, regex=True)
    outdf['EpisodeNameChi'] = indf['episodeNameChi'].replace('^-$',np.nan, regex=True)
    outdf['EpisodeNameEng'] = indf['episodeNameEng'].replace('^-$',np.nan, regex=True)
    outdf['EpisodeNameEng'] = outdf['EpisodeNameEng'].replace('Episode Number \d+', np.nan, regex=True)
    outdf['SynopsisChi'] = indf['synopsisChi'].replace('^-$',np.nan, regex=True)
    outdf['SynopsisChi'].fillna(indf['synopsisChi1'].replace('^-$',np.nan, regex=True), inplace=True)
    outdf['Classification'] = indf['classification'].replace('^-$',np.nan, regex=True)
    outdf["Premier"] = indf['premier'].replace('^-$',np.nan, regex=True)
    outdf["Premier"] = outdf["Premier"].apply(lambda x : 'Y' if x else 'N')
    outdf["Premiere"] = outdf["Premier"]
    outdf["Genre"] = indf['genre'].replace('^-$',np.nan, regex=True)
    outdf["SubGenre"] = indf['subGenre'].replace('^-$',np.nan, regex=True)
    outdf["EpisodeNo"] = indf['episodeNo'].replace('^-$',np.nan, regex=True)
    outdf["SeasonNo"] = indf['seasonNo'].replace('^-$',np.nan, regex=True)
    for i in outdf["EpisodeNo"][outdf["EpisodeNo"].notnull()]:
        try:
            test = pd.Series([i]).astype(int)
        except ValueError:
            temp = outdf["EpisodeNo"][outdf["EpisodeNo"].notnull()]
            cnt = temp[temp == i].index[0]
            raise ValueError("Episode# line "+str(cnt)+" occured error!")
    for i in outdf["SeasonNo"][outdf["SeasonNo"].notnull()]:
        try:
            test = Series([i]).astype(int)
        except ValueError:
            temp = outdf["SeasonNo"][outdf["SeasonNo"].notnull()]
            cnt = temp[temp == i].index[0]
            raise ValueError("Series No line "+str(cnt)+" occured error!")
    outdf["OriginalLang"] = indf['originalLang'].replace('^-$',np.nan, regex=True)
    if 'bilingual' in indf.columns:
        outdf["Bilingual"] = indf['bilingual']
    outdf["PortraitImage"] = indf["portraitImage"].replace('^-$',np.nan, regex=True)
    outdf["IsEpisodic"] = indf['isEpisodic']
    outdf["IsEpisodic"] = outdf[["IsEpisodic", "BrandNameEng"]].apply(lambda x: "Y" if x["BrandNameEng"] == "Midday Market Wrap" or x["BrandNameEng"] == "Smart Investor" else x["IsEpisodic"], axis=1)
    if 'isVOD' in indf.columns:
        outdf["IsVOD"] = indf['isVOD']
    outdf["CPInternalUniqueID"] = indf['cpInternalUniqueID'][indf['cpInternalUniqueID'].notnull()].astype(int).astype(str)
    outdf["MediaID"] = indf['mediaID']
    if 'hktamChannelId' in indf.columns:
        outdf['HKTAMChannelID'] = indf['hktamChannelId'].replace('^-$',np.nan, regex=True).apply(lambda x: str(int(re.match("^([0-9]+)(\.0*)?$", str(x)).groups()[0])) if re.match("^[0-9]+(\.0*)?$", str(x)) else None)
    if 'hktamProgramId' in indf.columns:
        outdf['HKTAMProgramID'] = indf['hktamProgramId'].replace('^-$',np.nan, regex=True).apply(lambda x: str(int(re.match("^([0-9]+)(\.0*)?$", str(x)).groups()[0])) if re.match("^[0-9]+(\.0*)?$", str(x)) else None)
    if 'hktamEpisodeId' in indf.columns:
        outdf['HKTAMEpisodeID'] = indf['hktamEpisodeId'].replace('^-$',np.nan, regex=True).apply(lambda x: str(int(re.match("^([0-9]+)(\.0*)?$", str(x)).groups()[0])) if re.match("^[0-9]+(\.0*)?$", str(x)) else None)
    if 'isLive' in indf.columns:
        outdf["IsLive"]=(indf['isLive'] == "T").map({True:"Y", False:"N"})
    outdf["IsNPVRProg"] = indf['isNPVRProg'].replace('^-$',np.nan, regex=True)
    from datetime import timedelta
    txDate = outdf[['TXDate', 'ActualTime']].apply(lambda x: ' '.join(x), axis=1).apply(lambda x: pd.to_datetime(str(x), format="%Y%m%d %H:%M"))
    txDate = txDate.apply(lambda x: x - timedelta(days=1) if x.hour < 6 else x).apply(lambda x: x.strftime("%Y%m%d"))
    outdf["EpisodeNameChi"].fillna(txDate[outdf['BrandNameEng'] == "About Sports"], inplace=True)
    outdf["EpisodeNameChi"].fillna(txDate[outdf['BrandNameEng'] == "15 mins Enews"], inplace=True)
    outdf["EpisodeNameEng"] = outdf[["TXDate", "ActualTime","BrandNameEng","EpisodeNameEng"]].apply(lambda x: x["TXDate"]+" "+x["ActualTime"] if x["BrandNameEng"] == "Smart Investor" else x["TXDate"] if x["BrandNameEng"] == "Midday Market Wrap" else x["EpisodeNameEng"], axis=1)                                  
    outdf["EpisodeNameChi"] = outdf[["TXDate", "ActualTime","BrandNameEng","EpisodeNameChi"]].apply(lambda x: x["TXDate"]+" "+x["ActualTime"] if x["BrandNameEng"] == "Smart Investor" else x["TXDate"] if x["BrandNameEng"] == "Midday Market Wrap" else x["EpisodeNameChi"], axis=1) 
    outdf["SponsorTextStuntChi"].fillna(outdf["SponsorTextStuntEng"], inplace=True)
    outdf["SponsorTextStuntEng"].fillna(outdf["SponsorTextStuntChi"], inplace=True)
    outdf["BrandNameChi"].fillna(outdf["BrandNameEng"], inplace=True)
    outdf["BrandNameEng"].fillna(outdf["BrandNameChi"], inplace=True)
    outdf["SeasonNameChi"].fillna(outdf["SeasonNameEng"], inplace=True)
    outdf["SeasonNameEng"].fillna(outdf["SeasonNameChi"], inplace=True)
    outdf["EpisodeNameChi"].fillna(outdf["EpisodeNameEng"], inplace=True)
    outdf["EpisodeNameEng"].fillna(outdf["EpisodeNameChi"], inplace=True)
    outdf["SynopsisChi"].fillna(outdf["SynopsisEng"], inplace=True)
    outdf["SynopsisEng"].fillna(outdf["SynopsisChi"], inplace=True)
    outdf["ShortSynopsisEng"] = outdf["SynopsisEng"][outdf["SynopsisEng"].notnull()].apply(lambda x: x[0:dc.settings.char_limit.get("ShortSynopsisEng")])
    outdf["ShortSynopsisChi"] = outdf["SynopsisChi"][outdf["SynopsisChi"].notnull()].apply(lambda x: x[0:dc.settings.char_limit.get("ShortSynopsisChi")])
    outdf["ShortSynopsisChi"].fillna(outdf["ShortSynopsisEng"], inplace=True)
    outdf["ShortSynopsisEng"].fillna(outdf["ShortSynopsisChi"], inplace=True)
    outdf["Genre"] = outdf["Genre"][outdf["Genre"].notnull()].str.lower().map(dc.mapping.genreMapping).fillna(outdf["Genre"])
    outdf["SubGenre"] = outdf["SubGenre"][outdf["SubGenre"].notnull()].str.lower().map(dc.mapping.genreMapping).fillna(outdf["SubGenre"])
    outdf["OriginalLang"] = outdf["OriginalLang"][outdf["OriginalLang"].notnull()].str.lower().map(dc.mapping.audioLangMapping).fillna(outdf["OriginalLang"])
    outdf["SeasonNo"] = outdf["SeasonNo"][outdf["SeasonNo"].notnull()].astype(int).astype(str)
    outdf["EpisodeNo"] = outdf["EpisodeNo"][outdf["EpisodeNo"].notnull()].astype(int).astype(str)
    text_column = ["SponsorTextStuntEng", "SponsorTextStuntChi", "BrandNameEng", "BrandNameChi", "EditionVersionEng", "EditionVersionChi", "SeasonNameEng", "SeasonNameChi", "EpisodeNameEng", "EpisodeNameChi", "SynopsisEng", "SynopsisChi", "ShortSynopsisEng", "ShortSynopsisChi", "DirectorProducerEng", "DirectorProducerChi", "CastEng", "CastChi"]
    for column in text_column:
        outdf[column] = outdf[column].astype(str).where(outdf[column].notnull()).apply(lambda x: dc.utils.jianfan(x) if isinstance(x, str) else x)
        outdf[column] = outdf[column].astype(str).where(outdf[column].notnull()).str.translate(dc.mapping.charTranslateTable)
    outdf[["FullTitleEng", "FullTitleChi"]] = outdf.apply(dc.utils.constructFullTitle, axis=1)
    outdf[["CID", "SID"]] = outdf.apply(dc.APIWrapper.constructID, axis=1)
    dc.utils.translateChar(outdf)
    dc.utils.chopCharacters(outdf)


    # Append EOF
    dc.utils.appendEOF(outdf)


    # Fill Channel #
    outdf['OwnerChannel'] = owner_channel
    outdf['ChannelNo'] = outdf['OwnerChannel'].replace(dc.APIMapping.ownerChannelMapping)

    # Write output file
    return outdf

def convert_nonlinear(in_io, out_io, format="preV1506", owner_channel="ViuTV"):
    "Convert nonlinear for ViuTV"
    return None

