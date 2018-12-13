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

def convert_linear(in_io, out_io, out_format="preV1506", owner_channel="Now Premier League 1"):
    "Convert linear schedule for Now Premier League 1"

    # Read input file
    indf = pd.read_json(in_io)
    indf.dropna(how="all", inplace=True)

    
    # Create output DataFrame
    outdf = pd.DataFrame(columns=dc.header.internalHeader)

    # Extract data from full title


    # Fill output DataFrame with related info
    indf["synopsisEng"] = indf["synopsisEng"].apply(lambda x: "" if x == "-" else x)
    indf["synopsisEng1"] = indf["synopsisEng1"].apply(lambda x: "" if x == "-" else x)
    indf["shortSynopsisEng"] = indf["shortSynopsisEng"].apply(lambda x: "" if x == "-" else x)
    indf["shortSynopsisEng1"] = indf["shortSynopsisEng1"].apply(lambda x: "" if x == "-" else x)
    indf["episodeNameChi"] = indf["episodeNameChi"].apply(lambda x: "" if x == "-" else x)
    indf["sponsorTextStuntEng"] = indf["sponsorTextStuntEng"].apply(lambda x: "" if x == "-" else x)
    indf["sponsorshipStartDate"] = indf["sponsorshipStartDate"].apply(lambda x: "" if x == "-" else x)
    indf["sponsorshipEndDate"] = indf["sponsorshipEndDate"].apply(lambda x: "" if x == "-" else x)
    indf["seasonNo"] = indf["seasonNo"].apply(lambda x: "" if x == "-" else x)
    indf["seasonNameEng"] = indf["seasonNameEng"].apply(lambda x: "" if x == "-" else x)
    indf["seasonNameChi"] = indf["seasonNameChi"].apply(lambda x: "" if x == "-" else x)
    indf["editionVersionEng"] = indf["editionVersionEng"].apply(lambda x: "" if x == "-" else x)
    indf["editionVersionChi"] = indf["editionVersionChi"].apply(lambda x: "" if x == "-" else x)
    indf["isEpisodic"] = indf["isEpisodic"].apply(lambda x: "" if x == "-" else x)
    indf["portraitImage"] = indf["portraitImage"].apply(lambda x: "" if x == "-" else x)
    indf["firstReleaseYear"] = indf["firstReleaseYear"].apply(lambda x: "" if x == "-" else x)
    indf["mediaID"] = indf["mediaID"].apply(lambda x: "" if x == "-" else x)
    indf["brandNameEng"] = indf["brandNameEng"].apply(lambda x: "" if x == "-" else x)
    indf["brandNameChi"] = indf["brandNameChi"].apply(lambda x: "" if x == "-" else x)
    indf["episodeNameEng"] = indf["episodeNameEng"].apply(lambda x: "" if x == "-" else x)
    indf["episodeNo"] = indf["episodeNo"].apply(lambda x: re.match("^([0-9]+)(\.0*)?$", str(x)).groups()[0] if re.match("^([0-9]+)(\.0*)?$", str(x)) else x)
    indf["episodeNameChi"] = indf["episodeNameChi"].apply(lambda x: re.match("^([0-9]+)(\.0*)?$", str(x)).groups()[0] if re.match("^([0-9]+)(\.0*)?$", str(x)) else x)
    indf["brandNameEng1"] = indf["brandNameEng1"].apply(lambda x: "" if x == "-" else x)
    indf["brandNameChi1"] = indf["brandNameChi1"].apply(lambda x: "" if x == "-" else x)
    channel_no = dc.mapping.ownerChannelMapping.get(owner_channel, owner_channel)
    outdf["SponsorTextStuntEng"] = indf["sponsorTextStuntEng"]
    outdf["SponsorTextStuntChi"] = indf["sponsorTextStuntEng"]
    outdf["EditionVersionEng"] = indf["editionVersionEng"]
    outdf["EditionVersionChi"] = indf["editionVersionChi"]
    outdf["SeasonNo"] = indf["seasonNo"][np.logical_and(indf["seasonNo"].notnull(), indf["seasonNo"].astype('str') != "")]
    outdf["SeasonNameEng"] = indf["seasonNameEng"]
    outdf["SeasonNameChi"] = indf["seasonNameChi"]
    outdf["Genre"] = indf["genre"]
    outdf["SubGenre"] = indf["subGenre"]
    outdf["FirstReleaseYear"] = indf["firstReleaseYear"]
    outdf["IsEpisodic"] = indf["isEpisodic"]
    outdf["PortraitImage"] = indf['portraitImage']
    outdf["TXDate"] = indf["txDate"][indf["txDate"].notnull()].apply(lambda x: pd.to_datetime(x, format="%Y-%m-%d").strftime('%Y%m%d'))
    outdf["ActualTime"] = indf["actualTime"][indf["actualTime"].notnull()].astype('str').str.slice(-8).str.slice(0, 5).str.zfill(5)
    outdf["BrandNameEng"] = indf["brandNameEng"].apply(lambda x: np.NaN if str(x).strip()=="" else x)
    outdf["BrandNameEng"].fillna(indf["brandNameEng1"], inplace=True)
    outdf["BrandNameChi"] = indf["brandNameEng1"].apply(lambda x: np.NaN if str(x).strip()=="" else x)
    outdf["BrandNameChi"].fillna(indf["brandNameChi1"], inplace=True)
    outdf["BrandNameEng"] = outdf["BrandNameEng"].apply(lambda x: np.NaN if str(x).strip()=="" else x)
    outdf["BrandNameChi"] = outdf["BrandNameChi"].apply(lambda x: np.NaN if str(x).strip()=="" else x)
    outdf["BrandNameEng"].fillna(outdf["BrandNameChi"], inplace=True)
    outdf["BrandNameChi"].fillna(outdf["BrandNameEng"], inplace=True)
    outdf["EpisodeNo"] = indf[["episodeNo","episodeNameEng"]].apply(lambda x: "" if (pd.notnull(x["episodeNameEng"]) and re.match("^\s*Week\s+(\d+)\s*$", str(x["episodeNameEng"]), re.I) is not None and re.search("^\s*Week\s+(\d+)\s*$", str(x["episodeNameEng"]), re.I).group(1) ==  str(x["episodeNo"])) or str(x["episodeNo"]) == "-" else x["episodeNo"],axis=1)
    outdf["EpisodeNameEng"] = indf[["episodeNo","episodeNameEng"]].apply(lambda x: "" if "Episode Number "+str(x["episodeNo"]) == str(x["episodeNameEng"]) or re.match("^\s*Episode Number\s*\d*\s*$", str(x["episodeNameEng"]),re.I) is not None else x["episodeNameEng"],axis=1)
    outdf["EpisodeNameEng"] = outdf["EpisodeNameEng"][outdf["EpisodeNameEng"].notnull()].astype('str').str.replace("(?i)\(Bilingual\)","").str.strip().astype('str').str.replace("\(粵\/英\)","").str.strip()
    outdf["EpisodeNameEng"] = outdf["EpisodeNameEng"].apply(lambda x: np.NaN if str(x).strip()=="" else x).fillna(indf["episodeNameChi"][~indf["episodeNameChi"].astype(str).str.match("^\s*(?i)Episode Number \d*\s*$")])
    outdf["EpisodeNameChi"] = indf["episodeNameChi"]
    outdf["EpisodeNameChi"] = outdf["EpisodeNameChi"][outdf["EpisodeNameChi"].notnull()].astype('str').str.replace("(?i)\(Bilingual\)","").str.strip().astype('str').str.replace("\(粵\/英\)","").str.strip()
    outdf["EpisodeNameChi"] = outdf["EpisodeNameChi"].apply(lambda x: np.NaN if str(x).strip()=="" else x).fillna(indf["episodeNameEng"][~indf["episodeNameEng"].astype(str).str.match("^\s*(?i)Episode Number \d*\s*$")])
    outdf["Premier"] = np.where(indf["premier"].astype(str).str.upper() == "True".upper() , "Y",                             np.where(indf["premier"].astype(str).str.upper() == "False".upper(), "N",                             "")                            )
    outdf["IsLive"] = np.where(indf["sourceDescription"].apply(lambda x: re.sub('\s','',str(x)).upper() == "LiveEvent".upper()) , "Y", "N")
    outdf["Genre"] = indf["genre"].apply(lambda x: dc.mapping.genreMapping.get(x.lower(), x) if pd.notnull(x) else None)
    outdf["SubGenre"] = indf["subGenre"]
    outdf["SubGenre"] = outdf[["SubGenre", "EpisodeNameEng", "EpisodeNameChi"]].apply(lambda x: str(x["SubGenre"])+"/Match" 
                                if (re.match(".*\sv\s.*", str(x["EpisodeNameEng"]),re.I) is not None or re.match(".*\s對\s.*", str(x["EpisodeNameChi"]),re.I) is not None) and "Match".upper() not in re.sub('\s','',str(x["SubGenre"])).upper().split("/") and pd.notnull(x["SubGenre"]) and str(x["SubGenre"]).strip() != ''
                                else "Match" if (re.match(".*\sv\s.*", str(x["EpisodeNameEng"]),re.I) is not None or re.match(".*\s對\s.*", str(x["EpisodeNameChi"]),re.I) is not None) and (pd.isnull(x["SubGenre"]) or str(x["SubGenre"]).strip() == '')
                                else x["SubGenre"], axis = 1)
    outdf["Bilingual"] = np.where(indf["episodeNameEng"].astype(str).str.match(".*(?i)\(Bilingual\).*"), "Y", "N")
    outdf["FirstReleaseYear"] = indf["firstReleaseYear"]
    outdf["IsEpisodic"] = indf["isEpisodic"]
    outdf["IsEpisodic"] = outdf[["IsEpisodic","SubGenre","EpisodeNo","EpisodeNameEng","EpisodeNameChi"]].apply(lambda x : "N" if ("Match".upper() in re.sub('\s','',str(x["SubGenre"])).upper().split("/")) or 
                                                                                                            (("Match".upper() not in re.sub('\s','',str(x["SubGenre"])).upper().split("/")) and (pd.isnull(x["EpisodeNo"]) or str(x["EpisodeNo"]).strip() == "") and (pd.isnull(x["EpisodeNameEng"]) or str(x["EpisodeNameEng"]).strip() == "") and (pd.isnull(x["EpisodeNameChi"]) or str(x["EpisodeNameChi"]).strip() == "")) 
                                                                                                               else "Y" if (pd.notnull(x["EpisodeNo"]) and str(x["EpisodeNo"]).strip() != "") or (pd.notnull(x["EpisodeNameEng"]) and str(x["EpisodeNameEng"]).strip() != "") or (pd.notnull(x["EpisodeNameChi"]) and str(x["EpisodeNameChi"]).strip() != "") else x["IsEpisodic"],axis=1)
    outdf["PortraitImage"] = indf["portraitImage"]
    outdf["Recordable"] = "Y"
    if channel_no not in ["616","617","618","619","682","683","684","630","680"]:
        outdf["IsNPVRProg"] = np.where(np.logical_and(indf["sourceDescription"].apply(lambda x: re.sub('\s','',str(x)).upper() == "LiveEvent".upper()),np.logical_or(
        outdf["SubGenre"].apply(lambda x: (re.sub("\s","",str(x)).upper() == "LaLiga/Soccer/Match".upper()) 
                                       or (re.sub("\s","",str(x)).upper() == "LaLiga/Match/Soccer".upper())
                                       or (re.sub("\s","",str(x)).upper() == "Soccer/LaLiga/Match".upper())
                                       or (re.sub("\s","",str(x)).upper() == "Soccer/Match/LaLiga".upper())
                                       or (re.sub("\s","",str(x)).upper() == "Match/LaLiga/Soccer".upper())
                                       or (re.sub("\s","",str(x)).upper() == "Match/Soccer/LaLiga".upper())),
        outdf["SubGenre"].apply(lambda x: (re.sub("\s","",str(x)).upper() == "PL/Soccer/Match".upper())
                                       or (re.sub("\s","",str(x)).upper() == "Soccer/PL/Match".upper())
                                       or (re.sub("\s","",str(x)).upper() == "Match/Soccer/PL".upper())
                                       or (re.sub("\s","",str(x)).upper() == "Soccer/Match/PL".upper())
                                       or (re.sub("\s","",str(x)).upper() == "Match/PL/Soccer".upper())
                                       or (re.sub("\s","",str(x)).upper() == "PL/Match/Soccer".upper())
                               ))),"Y", "N")
    elif channel_no not in ["682","683","684","630","680"]:
        outdf["IsNPVRProg"] = np.where(np.logical_and(indf["sourceDescription"].apply(lambda x: re.sub('\s','',str(x)).upper() == "LiveEvent".upper()),
        outdf["SubGenre"].apply(lambda x: ("Worldcup".upper() in re.sub('\s','',str(x)).upper().split("/"))
                                      and ("Soccer".upper() in re.sub('\s','',str(x)).upper().split("/"))
                                      and ("Match".upper() in re.sub('\s','',str(x)).upper().split("/")))
                                ),"Y", "N")
    outdf["IsRestartTV"] = "Y"
    outdf["EffectiveDate"] = ""
    if channel_no not in ["682","683","684","630","680"]:
        outdf.ix[(outdf["Recordable"] == "Y") & (outdf["IsNPVRProg"] == "N") & (outdf["IsRestartTV"] == "Y") & (indf["sourceDescription"].apply(lambda x: re.sub('\\s','',str(x)).upper() != "LiveEvent".upper())), "ExpirationDate"] = 1
    outdf["EpisodeNo"] = outdf[["EpisodeNo","EpisodeNameEng","EpisodeNameChi"]].apply(lambda x: "" if (pd.notnull(x["EpisodeNameEng"]) and str(x["EpisodeNameEng"]).strip() != "") or (pd.notnull(x["EpisodeNameChi"]) and str(x["EpisodeNameChi"]).strip() != "") else x["EpisodeNo"],axis=1)
    if channel_no in ["682","630"]:
        outdf["Recordable"] = ""
        outdf["IsNPVRProg"] = ""
        outdf["IsRestartTV"] = ""
        outdf["ExpirationDate"] = ""
    elif channel_no in ["683","684","680"]:
        outdf["IsNPVRProg"] = "N"
        outdf["IsRestartTV"] = "N"  
    text_column = ["SponsorTextStuntEng", "SponsorTextStuntChi", "BrandNameEng", "BrandNameChi", "EditionVersionEng", "EditionVersionChi", "SeasonNameEng", "SeasonNameChi", "EpisodeNameEng", "EpisodeNameChi", "SynopsisEng", "SynopsisChi", "ShortSynopsisEng", "ShortSynopsisChi", "DirectorProducerEng", "DirectorProducerChi", "CastEng", "CastChi"]
    for column in text_column:
        outdf[column] = outdf[column].astype(str).where(outdf[column].notnull()).str.translate(dc.mapping.charTranslateTable)
    outdf["ChannelNo"] = channel_no
    outdf = outdf[outdf[["BrandNameEng","BrandNameChi"]].apply(lambda x: False if re.sub("[\s]","",str(x["BrandNameEng"])).upper() == "ENDOFFILE" or re.sub("[\s]","",str(x["BrandNameChi"])).upper() == "ENDOFFILE" else True, axis = 1)]
    dc.utils.title_df(outdf)
    outdf[["FullTitleEng", "FullTitleChi"]] = outdf.apply(dc.utils.constructFullTitle, axis=1)
    outdf[["ProgrammeNameEng", "ProgrammeNameChi"]] = outdf.apply(dc.utils.constructProgrammeName, axis=1)
    dc.utils.translateChar(outdf)
    dc.utils.chopCharacters(outdf)


    # Append EOF
    dc.utils.appendEOF(outdf)


    # Fill Channel #
    outdf['OwnerChannel'] = owner_channel
    outdf['ChannelNo'] = outdf['OwnerChannel'].replace(dc.APIMapping.ownerChannelMapping)

    # Write output file
    return outdf

def convert_nonlinear(in_io, out_io, format="preV1506", owner_channel="Now Premier League 1"):
    "Convert nonlinear for Now Premier League 1"
    return None

