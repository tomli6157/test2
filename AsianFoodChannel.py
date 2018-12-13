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

def convert_linear(in_io, out_io, out_format="preV1506", owner_channel="Asian Food Channel"):
    "Convert linear schedule for Asian Food Channel"

    # Read input file
    import dc.TestReadingExcel
    from pandas.compat import (lrange)
    data = dc.TestReadingExcel.read_excel(in_io, sheetname=0)
    rowSize = 0
    isColumn = False
    for i in data:
        colSize = 0
        for j in i:
            if (j == 'Channel #'):
                isColumn = True
                break
            colSize +=1
        if isColumn:
            break
        rowSize +=1
    if type(in_io) != str:
        in_io.seek(0)
    indf = pd.read_excel(in_io, sheetname=0, parse_cols=lrange(colSize,len(data[0])) , skiprows=rowSize)

    
    # Create output DataFrame
    outdf = pd.DataFrame(columns=dc.header.internalHeader)

    # Extract data from full title
    tempExtractedInfo = [
        indf["Season no. (max 3 digits)"][indf["Season no. (max 3 digits)"].notnull()].astype('str').str.strip().str.translate(dc.mapping.charTranslateTable).str.extract("(?P<BrandNameEng>.*?)\s*((,\s*The\s*)*S(?P<SeasonNo>\d*)?)?(,\s*The\s*)*$", expand=False)              
    ]


    # Fill output DataFrame with related info
    outdf["ActualTime"] = indf["Actual Start Time"].apply(lambda x: pd.to_datetime(x, format="%H:%M:%S"))
    outdf["TXDate"] = indf["TX Date"].apply(lambda x: pd.to_datetime(x, format="%Y%m%d")).apply(lambda x:x.strftime("%Y%m%d"))
    outdf["ActualTime"] = outdf["ActualTime"].astype('str').str.slice(-8).str.slice(0, 5).str.zfill(5)
    outdf["SponsorTextStuntEng"]=indf["Sponsor text / Stunt (English)"][indf["Sponsor text / Stunt (English)"].notnull()].astype(str).str.translate(dc.mapping.charTranslateTable)
    outdf["SponsorTextStuntChi"]=indf["Sponsor text / Stunt (Chinese)"][indf["Sponsor text / Stunt (Chinese)"].notnull()].astype(str).str.translate(dc.mapping.charTranslateTable)
    outdf["BrandNameEng"]=indf["Brand name (English)"][indf["Brand name (English)"].notnull()].astype('str').str.translate(dc.mapping.charTranslateTable).apply(lambda x: x[0:dc.settings.char_limit.get("BrandNameEng")])     
    outdf["BrandNameEng"].fillna(tempExtractedInfo[0]["BrandNameEng"], inplace=True) 
    outdf["BrandNameChi"]=indf["Brand name (Chinese)"][indf["Brand name (Chinese)"].notnull()].astype('str').str.translate(dc.mapping.charTranslateTable).apply(lambda x: x[0:dc.settings.char_limit.get("BrandNameChi")])
    outdf["BrandNameChi"].fillna(outdf["BrandNameEng"], inplace=True)
    outdf["BrandNameEng"].fillna(outdf["BrandNameChi"], inplace=True)
    outdf["EditionVersionEng"]=indf["Edition / Version (English)"]
    outdf["EditionVersionChi"]=indf["Edition / Version (Chinese)"]
    outdf["SeasonNo"]=indf["Season no. (max 3 digits)"].apply(lambda x: re.match("^([0-9]+)(\.0+)?$", str(x)).groups()[0] if re.match("^[0-9]+(\.0+)?$", str(x)) else np.NaN)        
    outdf["SeasonNo"].fillna(tempExtractedInfo[0]["SeasonNo"], inplace=True)
    outdf["SeasonNameEng"]=indf["Season Name (English)"][indf["Season Name (English)"].notnull()].astype(str).str.translate(dc.mapping.charTranslateTable).apply(lambda x: x[0:dc.settings.char_limit.get("SeasonNameEng")])
    outdf["SeasonNameChi"]=indf["Season Name (Chinese)"][indf["Season Name (Chinese)"].notnull()].astype(str).str.translate(dc.mapping.charTranslateTable).apply(lambda x: x[0:dc.settings.char_limit.get("SeasonNameChi")])
    outdf["EpisodeNo"]=indf["Episode no. (max 5 digits)"]
    outdf["EpisodeNameEng"]=indf["Episode name (English)"][indf["Episode name (English)"].notnull()].astype(str).str.translate(dc.mapping.charTranslateTable).apply(lambda x: x[0:dc.settings.char_limit.get("EpisodeNameEng")])
    outdf["EpisodeNameChi"]=indf["Episode name (Chinese)"][indf["Episode name (Chinese)"].notnull()].astype(str).str.translate(dc.mapping.charTranslateTable).apply(lambda x: x[0:dc.settings.char_limit.get("EpisodeNameChi")])
    outdf["SynopsisEng"]=indf["Synopsis (English)"][indf["Synopsis (English)"].notnull()].astype(str).str.translate(dc.mapping.charTranslateTable).apply(lambda x: x[0:dc.settings.char_limit.get("SynopsisEng")])
    outdf["SynopsisChi"]=indf["Synopsis (Chinese)"][indf["Synopsis (Chinese)"].notnull()].astype(str).str.translate(dc.mapping.charTranslateTable).apply(lambda x: x[0:dc.settings.char_limit.get("SynopsisChi")])
    outdf["SynopsisEng"].fillna(outdf["SynopsisChi"], inplace=True)
    outdf["SynopsisChi"].fillna(outdf["SynopsisEng"], inplace=True)
    outdf["ShortSynopsisEng"]=indf["Short Synopsis (English) (max 120 chars)"][indf["Short Synopsis (English) (max 120 chars)"].notnull()].apply(lambda x: x[0:dc.settings.char_limit.get("ShortSynopsisEng")])
    outdf["ShortSynopsisChi"]=indf["Short Synopsis (Chinese) (max 120 chars)"][indf["Short Synopsis (Chinese) (max 120 chars)"].notnull()].apply(lambda x: x[0:dc.settings.char_limit.get("ShortSynopsisChi")])
    outdf["PremierOld"] = "0"
    outdf["Premier"] = indf["Premiere (Y/N)"].replace("Yes","Y")
    outdf["IsLive"] = indf["Is Live (Y/N)"]
    outdf["IsEpisodic"] = (outdf["EpisodeNo"].notnull() | outdf["EpisodeNameEng"].notnull() | outdf["EpisodeNameChi"].notnull()).map({True:"Y", False:"N"})
    outdf["Classification"]=indf["Classification"]
    outdf["Genre"] = indf["Genre"][indf["Genre"].notnull()].str.lower().map(dc.mapping.genreMapping).fillna(indf["Genre"])
    outdf["SubGenre"] = indf["Sub-Genre"][indf["Sub-Genre"].notnull()].str.lower().map(dc.mapping.genreMapping).fillna(indf["Sub-Genre"])
    outdf["DirectorProducerEng"] = indf["Director / Producer (English)"][indf["Director / Producer (English)"].notnull()].astype(str).str.translate(dc.mapping.charTranslateTable).apply(lambda x: str(x).strip()).str.replace('(\s*,\s*|\s*、\s*|\s*，\s*)', ', ').str.translate(dc.mapping.charTranslateTable).apply(lambda x: x[0:dc.settings.char_limit.get("DirectorProducerEng")])
    outdf["DirectorProducerChi"] = indf["Director / Producer (Chinese)"][indf["Director / Producer (Chinese)"].notnull()].astype(str).str.translate(dc.mapping.charTranslateTable).apply(lambda x: str(x).strip()).str.replace('(\s*,\s*|\s*、\s*|\s*，\s*)', ', ').str.translate(dc.mapping.charTranslateTable).apply(lambda x: x[0:dc.settings.char_limit.get("DirectorProducerChi")])
    outdf["CastEng"] = indf["Cast (English)"][indf["Cast (English)"].notnull()]
    outdf["CastChi"] = indf["Cast (Chinese)"][indf["Cast (Chinese)"].notnull()]
    outdf["OriginalLang"] = indf["Original language (max 50 chars)"].astype(str).str.split('; ').apply(lambda audioLangs: [dc.mapping.audioLangMapping.get(audioLang.lower(), audioLang) for audioLang in audioLangs]).str.join(', ')
    outdf["AudioLang"] = indf["Audio language"][indf["Audio language"].notnull()].astype(str).str.split('; ').apply(lambda audioLangs: [dc.mapping.audioLangMapping.get(audioLang.lower(), audioLang) for audioLang in audioLangs]).str.join(', ')
    outdf["SubtitleLang"] = indf["Subtitle language"][indf["Subtitle language"].notnull()].astype(str).str.split('; ').apply(lambda subtitleLangs: [dc.mapping.subtitleMapping.get(subtitleLang.lower(),subtitleLang) for subtitleLang in subtitleLangs]).str.join(', ')
    outdf["RegionCode"] = indf["Region Code (max 2 chars)"][indf["Region Code (max 2 chars)"].notnull()].astype(str)
    outdf["FirstReleaseYear"] = indf["First release year (max 4 digits)"].astype(str)
    outdf["PortraitImage"] = indf["Portrait Image"]
    outdf["ImageWithTitle"] = indf["Image with title  (Y/N)"]
    outdf["RecordableForCatchUp"] = indf["Recordable for Catch-up (Y/N)"]
    outdf["CPInternalUniqueID"] = indf["CP Internal unique ID"]
    outdf["CPInternalSeriesID"] = indf[" CP Internal Series ID"]
    dc.utils.title_df(outdf)
    outdf[["FullTitleEng", "FullTitleChi"]] = outdf.apply(dc.utils.constructFullTitle, axis=1)
    outdf[["ProgrammeNameEng", "ProgrammeNameChi"]] = outdf.apply(dc.utils.constructProgrammeName, axis=1)
    dc.utils.translateChar(outdf)
    dc.utils.chopCharacters(outdf)


    # Append EOF
    dc.utils.appendEOF(outdf)


    # Fill Channel #
    outdf['OwnerChannel'] = owner_channel
    outdf['ChannelNo'] = outdf['OwnerChannel'].replace(dc.mapping.ownerChannelMapping)

    # Write output file
    dc.io.to_io(outdf, out_io, out_format)

def convert_nonlinear(in_io, out_io, format="preV1506", owner_channel="Asian Food Channel"):
    "Convert nonlinear for Asian Food Channel"
    return None

