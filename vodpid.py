import pandas as pd
import Levenshtein
import dc.header
import dc.utils
import dc.mapping
import re

# This should be done only once when web app is first started
vod_db = pd.read_pickle("dc/data/vod_db.p")

def is_element_same(a, b, case_sensitive=True, rm_symbols=False):
    a = a if (pd.isnull(a) or isinstance(a, str)) else int(a) 
    b = b if (pd.isnull(b) or isinstance(b, str)) else int(b)
    if case_sensitive:
        if not rm_symbols:
            return (pd.isnull(a) and pd.isnull(b)) or (str(a).strip() == str(b).strip())
        else:
            return (pd.isnull(a) and pd.isnull(b)) or (re.sub("\s","",str(a)).translate(dc.mapping.VODSymbols) == re.sub("\s","",str(b)).translate(dc.mapping.VODSymbols))
    else:
        if not rm_symbols:
            return (pd.isnull(a) and pd.isnull(b)) or (str(a).strip().lower() == str(b).strip().lower())
        else:
            return (pd.isnull(a) and pd.isnull(b)) or (re.sub("\s","",str(a)).translate(dc.mapping.VODSymbols).lower() == re.sub("\s","",str(b)).translate(dc.mapping.VODSymbols).lower())

def get_similarity(record, query_string, query_string_chi):
    if "(closed)" in record["FULL_TITLE_ENG"].lower():
        return 0
    return (Levenshtein.ratio(query_string.lower(), record["FULL_TITLE_ENG"].strip().lower()) + Levenshtein.ratio(query_string_chi.lower(), record["FULL_TITLE_CHI"].strip().lower())) / 2

def pick_candidate(vod_library, candidate_index, record):
    idxmax = None
    vod_candidate = vod_library.ix[candidate_index]
    compare_map = {
                    "OTHERS" : "OpeningSlate",
                    "AUDIO" : "AudioSlate",
                    "ADVISORY" : "AdvisorySlate",
                    "CLASS" : "TVClassification",
                    "SECCLASS" : "WebClassification"                
                  }
    s = vod_candidate.apply(lambda candidate: sum([is_element_same(candidate[key], record[value]) for key, value in compare_map.items()]), axis=1)

    # If there are still multiple candidates, pick the one with larger PID (i.e. latest one)
    vod_candidate = vod_candidate[s==s.max()]
    idxmax = vod_candidate["PID"].idxmax()
        
    return idxmax

def find_similar_db_record(record, vod_library, serviceId):
    query_string = record["FullTitleEngOld"]
    if pd.isnull(query_string):
        query_string = record[0]      # Use constructed Eng full title
    query_string = str(query_string).strip()
    
    query_string_chi = record["FullTitleChiOld"]
    if pd.isnull(query_string_chi):
        query_string_chi = record[1]  # Use constructed Chi full title
    query_string_chi = str(query_string_chi).strip()

    # Try to find if there is any exact match for both English & Chinese full title
    max_ratio = 0
    idxmax = None
    
#    exact_match = vod_library[vod_library[["FULL_TITLE_ENG", "FULL_TITLE_CHI"]].apply(lambda x: (x["FULL_TITLE_ENG"].lower() == query_string.lower()) and (x["FULL_TITLE_CHI"].lower() == query_string_chi.lower()), axis=1)]
    compare_map = {
                    'SPONSOR_ENG': 'SponsorTextStuntEng',
                    'SPONSOR_CHI': 'SponsorTextStuntChi',
                    'BRAND_NAME_ENG': 'BrandNameEng',
                    'BRAND_NAME_CHI': 'BrandNameChi',
                    'SEASON_NUM': 'SeasonNo',
                    'PRODUCT_VERSION_ENG': 'EditionVersionEng',
                    'PRODUCT_VERSION_CHI': 'EditionVersionChi',
                    'SEASON_NAME_ENG': 'SeasonNameEng',
                    'SEASON_NAME_CHI': 'SeasonNameChi',
                    'EPISODE_NUM': 'EpisodeNo',
                    'EPISODE_NAME_U3_ENG': 'EpisodeNameEng',
                    'EPISODE_NAME_U3_CHI': 'EpisodeNameChi',
                    'EVENT_DATE': 'EventDate',
#                    'FIRST_RELEASE_YEAR': 'FirstReleaseYear'
                  }
#    exact_match = vod_library[vod_library.apply(lambda x: sum([((pd.isnull(x[key]) and pd.isnull(record[value])) or (str(x[key]).strip().lower() == str(record[value]).strip().lower())) for key, value in compare_map.items()]) == len(compare_map), axis=1)]
#    exact_match = vod_library[vod_library.apply(lambda x: sum([is_element_same(x[key], record[value], False) for key, value in compare_map.items()]) == len(compare_map), axis=1)]
#    exact_match = vod_library[vod_library.apply(lambda x: all(is_element_same(x[key], record[value], False) for key, value in compare_map.items()), axis=1)]
#    exact_match = vod_library[vod_library.apply(lambda x: all(is_element_same(x[key], record[value], False) for key, value in compare_map.items()), axis=1).astype(bool, copy=False)]
    exact_match_cond = vod_library.apply(lambda x: all(is_element_same(x[key], record[value], True, False) for key, value in compare_map.items()), axis=1)
    if (len(exact_match_cond) == 0) or exact_match_cond.dtypes != bool:
        exact_match_cond = exact_match_cond.astype(bool)
    exact_match = vod_library[exact_match_cond]
    if serviceId == 1:
        exact_match_cond1 = vod_library.apply(lambda x: all(is_element_same(x[key], record[value], False, True) for key, value in compare_map.items()), axis=1)
        if (len(exact_match_cond1) == 0) or exact_match_cond1.dtypes != bool:
            exact_match_cond1 = exact_match_cond1.astype(bool)
        exact_match1 = vod_library[exact_match_cond1]
    
    # If there is exact match
    if (len(exact_match.index) == 1):
        max_ratio = 1
        idxmax = exact_match.index[0]

    # if there are multiple exact matches
    elif (len(exact_match.index) > 1):
        max_ratio = 1
        idxmax = pick_candidate(vod_library, exact_match.index, record)
                    
    # If there is no exact match, perform approximate match for English full title by calculating levenshtein distance
#    else:
#        s = vod_library.apply(get_similarity, args=(query_string, query_string_chi), axis=1)
#        max_ratio = s.max()
#        candidates = vod_library[s >= max_ratio]
#        idxmax = pick_candidate(vod_library, candidates.index, record)
    elif (serviceId == 1 and len(exact_match1.index) == 1):
        max_ratio = 0.9
        idxmax = exact_match1.index[0]
    elif (serviceId == 1 and len(exact_match1.index) > 1):
        max_ratio = 0.9
        idxmax = pick_candidate(vod_library, exact_match1.index, record)

    # If there is no exact match, return nothing
    else:
        return pd.Series([None, None, None, None, None, None, None, None, None])

    # Get target record from DB record
    r = vod_library[["PID", "FULL_TITLE_ENG", "FULL_TITLE_CHI", "OTHERS", "AUDIO", "ADVISORY", "CLASS", "SECCLASS"]].ix[idxmax].tolist()
    r.append(max_ratio)
    
    return pd.Series(r)

def find_vodpid(in_io, out_io, threshold=0):
    # Read input data
    indf = pd.read_excel(in_io, sheetname=0, keep_default_na=False, na_values=[''])

    # Prepare output table header
    outdf = pd.DataFrame(columns=["ProductID_DB", "Score", "AllClassificationMatch", "FullTitleEng_DB", "FullTitleChi_DB", "LibraryEng", "LibraryChi", "FullTitleEngOld", "FullTitleChiOld", "BrandNameEng", "BrandNameChi", "EditionVersionEng", "EditionVersionChi", "SeasonNo", "SeasonNameEng", "SeasonNameChi", "EpisodeNo", "EpisodeNameEng", "EpisodeNameChi", "SponsorTextStuntEng", "SponsorTextStuntChi", "FirstReleaseYear", "EventDate", "IsLive", "OpeningSlate_DB", "OpeningSlate", "AudioSlate_DB", "AudioSlate", "AdvisorySlate_DB", "AdvisorySlate", "TVClassification_DB", "TVClassification", "WebClassification_DB", "WebClassification"])
    
    # Copy input file data to output
    for out_column in outdf.columns:
        in_column = dc.header.nlPostV1506HeaderMapping.get(out_column)
        if pd.notnull(in_column):
            if in_column in indf.columns:
                outdf[out_column] = indf[in_column]
            else:
                outdf[out_column] = None

    # Identify the library name to search PID from
    library_name = outdf["LibraryEng"].ix[0]    # Assume whole dataframe use same library_name
    vod_library = vod_db[(vod_db["LIBRARY_NAME_ENG"] == library_name) & (vod_db['STATUS'] == 'ACTIVE')]
    if not any((vod_db["LIBRARY_NAME_ENG"] == library_name).tolist()):
       raise Exception("Library name cannot be found in database")
    
    # Construct full title in case it is not available in input file
    outdf = outdf.join(outdf.apply(dc.utils.constructFullTitle, axis=1))
    
    # Find most similar DB record for each input file record
    outdf[["ProductID_DB", "FullTitleEng_DB", "FullTitleChi_DB", "OpeningSlate_DB", "AudioSlate_DB", "AdvisorySlate_DB", "TVClassification_DB", "WebClassification_DB", "Score"]] = outdf.apply(find_similar_db_record, args=(vod_library,0,), axis=1)
    
    # Mark whether all the classification fields are matched for the input record and the picked DB record
    outdf["AllClassificationMatch"] = outdf.apply(lambda r: is_element_same(r["OpeningSlate_DB"], r["OpeningSlate"]) and is_element_same(r["AudioSlate_DB"], r["AudioSlate"]) and is_element_same(r["AdvisorySlate_DB"], r["AdvisorySlate"]) and is_element_same(r["TVClassification_DB"], r["TVClassification"]) and is_element_same(r["WebClassification_DB"], r["WebClassification"]), axis=1).map({True: "Y", False: "N"})
    
    # Remove product id if not match all classification
    outdf["ProductID_DB"] = outdf["ProductID_DB"][outdf["AllClassificationMatch"] == "Y"]
    
    # Drop columns that is no use
    outdf.drop([0, 1, "IsLive", "LibraryEng", "LibraryChi"], axis=1, inplace=True)

    # Write to output IO
#    writer = pd.ExcelWriter(out_io, engine='xlsxwriter')
#    outdf.to_excel(writer, index=False)
#    writer.save()

    # Write to output IO
#    indf2 = pd.read_excel(in_io, sheetname=0, keep_default_na=False)
    outdf2 = outdf[["ProductID_DB", "Score", "AllClassificationMatch", ]].join(indf)
    # Convert PID to string
    outdf2['ProductID_DB'] = outdf2['ProductID_DB'].apply(lambda x: str(int(x)) if pd.notnull(x) else None)
    # Convert date time to string
    outdf2['Start Date (YYYYMMDD)'] = outdf2['Start Date (YYYYMMDD)'].apply(lambda x: x if pd.isnull(x)
                                                                            else (str(int(x)) if re.fullmatch("[\d]{8}(\.0)*", str(x)) or re.fullmatch("[\d]{12}(\.0)*", str(x))
                                                                            else (x.strftime('%Y%m%d') if not isinstance(x, str)
                                                                            else x)))
    outdf2['End Date (YYYYMMDD)'] = outdf2['End Date (YYYYMMDD)'].apply(lambda x: x if pd.isnull(x)
                                                                            else (str(int(x)) if re.fullmatch("[\d]{8}(\.0)*", str(x)) or re.fullmatch("[\d]{12}(\.0)*", str(x))
                                                                            else (x.strftime('%Y%m%d') if not isinstance(x, str)
                                                                            else x)))
    outdf2['Duration (HH:MM:SS)'] = outdf2['Duration (HH:MM:SS)'].apply(lambda x: x if pd.isnull(x)
                                                                            else (str(x) if re.fullmatch("\d\d:\d\d:\d\d", str(x))
                                                                            else (x.strftime('%H:%M:%S') if not isinstance(x, str)
                                                                            else x)))
    writer = pd.ExcelWriter(out_io, engine='xlsxwriter')
    outdf2.to_excel(writer, index=False)
    writer.save()

def find_vodpid1(in_io, out_io, threshold=0):
    # Read input data
    indf = pd.read_excel(in_io, sheetname=0, keep_default_na=False, na_values=[''])
    indf.dropna(how='all', inplace=True)
    
    # Prepare output table header
    outdf = pd.DataFrame(columns=["ProductID_DB", "Score", "AllClassificationMatch", "FullTitleEng_DB", "FullTitleChi_DB", "LibraryEng", "LibraryChi", "FullTitleEngOld", "FullTitleChiOld", "BrandNameEng", "BrandNameChi", "EditionVersionEng", "EditionVersionChi", "SeasonNo", "SeasonNameEng", "SeasonNameChi", "EpisodeNo", "EpisodeNameEng", "EpisodeNameChi", "SponsorTextStuntEng", "SponsorTextStuntChi", "FirstReleaseYear", "EventDate", "IsLive", "OpeningSlate_DB", "OpeningSlate", "AudioSlate_DB", "AudioSlate", "AdvisorySlate_DB", "AdvisorySlate", "TVClassification_DB", "TVClassification", "WebClassification_DB", "WebClassification"])
    
    # Copy input file data to output
    for out_column in outdf.columns:
        in_column = dc.header.nlPostV1506HeaderMapping.get(out_column)
        if pd.notnull(in_column):
            if in_column in indf.columns:
                outdf[out_column] = indf[in_column]
            else:
                outdf[out_column] = None

    # Identify the library name to search PID from
    library_name = outdf["LibraryEng"].ix[0]    # Assume whole dataframe use same library_name
    vod_library = vod_db[(vod_db["LIBRARY_NAME_ENG"] == library_name) & (vod_db['STATUS'] == 'ACTIVE')]
    if not any((vod_db["LIBRARY_NAME_ENG"] == library_name).tolist()):
       raise Exception("Library name cannot be found in database")
    
    # Construct full title in case it is not available in input file
    outdf = outdf.join(outdf.apply(dc.utils.constructFullTitle, axis=1))
    
    # Find most similar DB record for each input file record
    outdf[["ProductID_DB", "FullTitleEng_DB", "FullTitleChi_DB", "OpeningSlate_DB", "AudioSlate_DB", "AdvisorySlate_DB", "TVClassification_DB", "WebClassification_DB", "Score"]] = outdf.apply(find_similar_db_record, args=(vod_library,1,), axis=1)
    
    # Mark whether all the classification fields are matched for the input record and the picked DB record
    outdf["AllClassificationMatch"] = outdf.apply(lambda r: is_element_same(r["OpeningSlate_DB"], r["OpeningSlate"]) and is_element_same(r["AudioSlate_DB"], r["AudioSlate"]) and is_element_same(r["AdvisorySlate_DB"], r["AdvisorySlate"]) and is_element_same(r["TVClassification_DB"], r["TVClassification"]) and is_element_same(r["WebClassification_DB"], r["WebClassification"]) and (re.match("^1(\.0*){0,1}$",str(r["Score"])) is not None or str(r["Score"]) == '0.9'), axis=1).map({True: "Y", False: "N"})
    
    # Remove product id if not match all classification
    outdf["ProductID_DB"] = outdf[["ProductID_DB", "AllClassificationMatch", "Score"]].apply(lambda x: "EXACT MATCH" if re.match("^1(\.0*){0,1}$",str(x["Score"])) is not None and x["AllClassificationMatch"] == "Y" else x["ProductID_DB"] if str(x["Score"]) == '0.9' and x["AllClassificationMatch"] == "Y" else None,axis=1)                                                                                                                                         
    # Drop columns that is no use
    outdf.drop([0, 1, "IsLive", "LibraryEng", "LibraryChi"], axis=1, inplace=True)

    # Write to output IO
#    writer = pd.ExcelWriter(out_io, engine='xlsxwriter')
#    outdf.to_excel(writer, index=False)
#    writer.save()

    # Write to output IO
#    indf2 = pd.read_excel(in_io, sheetname=0, keep_default_na=False)
    #outdf2 = outdf[["ProductID_DB", "Score", "AllClassificationMatch", ]].join(indf)
    outdf2 = outdf[["ProductID_DB", "Score", "AllClassificationMatch", "FullTitleEng_DB", "FullTitleChi_DB", ]].join(indf)
    outdf2[["FullTitleEng_DB", "FullTitleChi_DB"]] = outdf2[["FullTitleEng_DB", "FullTitleChi_DB" , "Score"]].apply(lambda x:
                                    pd.Series([x["FullTitleEng_DB"],x["FullTitleChi_DB"]]) if str(x["Score"]) == "0.9" else pd.Series(["",""]),axis=1)
    # Convert PID to string
    outdf2['ProductID_DB'] = outdf2['ProductID_DB'].apply(lambda x: str(dc.utils.transferNumeric(x)) if pd.notnull(x) and str(x).strip() != '' and re.match("^[0-9]+$", str(dc.utils.transferNumeric(x))) is not None else x if str(x) == "EXACT MATCH" else None)
    # Convert date time to string
    outdf2['Start Date (YYYYMMDD)'] = outdf2['Start Date (YYYYMMDD)'].apply(lambda x: x if pd.isnull(x)
                                                                            else (str(int(x)) if re.fullmatch("[\d]{8}(\.0)*", str(x)) or re.fullmatch("[\d]{12}(\.0)*", str(x))
                                                                            else (x.strftime('%Y%m%d') if not isinstance(x, str)
                                                                            else x)))
    outdf2['End Date (YYYYMMDD)'] = outdf2['End Date (YYYYMMDD)'].apply(lambda x: x if pd.isnull(x)
                                                                            else (str(int(x)) if re.fullmatch("[\d]{8}(\.0)*", str(x)) or re.fullmatch("[\d]{12}(\.0)*", str(x))
                                                                            else (x.strftime('%Y%m%d') if not isinstance(x, str)
                                                                            else x)))
    outdf2['Duration (HH:MM:SS)'] = outdf2['Duration (HH:MM:SS)'].apply(lambda x: x if pd.isnull(x)
                                                                            else (str(x) if re.fullmatch("\d\d:\d\d:\d\d", str(x))
                                                                            else (x.strftime('%H:%M:%S') if not isinstance(x, str)
                                                                            else x)))
    outdf2.rename(columns={"FullTitleEng_DB":"Full Title in PWH database (Eng)"
                                  ,"FullTitleChi_DB":"Full Title in PWH database (Chi)"
                                  ,"ProductID_DB":"Similar ProductID in DB"},inplace=True)
    
    writer = pd.ExcelWriter(out_io, engine='xlsxwriter')
    outdf2.to_excel(writer, index=False)
    writer.save()

def find_vodpid2(in_io, out_io, threshold=0):
    # Read input data
    indf = pd.read_excel(in_io, sheetname=0, keep_default_na=False, na_values=[''])

    # Prepare output table header
    outdf = pd.DataFrame(columns=["ProductID_DB", "Score", "AllClassificationMatch", "WebClassificationMatch", "FullTitleEng_DB", "FullTitleChi_DB", "LibraryEng", "LibraryChi", "FullTitleEngOld", "FullTitleChiOld", "BrandNameEng", "BrandNameChi", "EditionVersionEng", "EditionVersionChi", "SeasonNo", "SeasonNameEng", "SeasonNameChi", "EpisodeNo", "EpisodeNameEng", "EpisodeNameChi", "SponsorTextStuntEng", "SponsorTextStuntChi", "FirstReleaseYear", "EventDate", "IsLive", "OpeningSlate_DB", "OpeningSlate", "AudioSlate_DB", "AudioSlate", "AdvisorySlate_DB", "AdvisorySlate", "TVClassification_DB", "TVClassification", "WebClassification_DB", "WebClassification"])
    
    # Copy input file data to output
    for out_column in outdf.columns:
        in_column = dc.header.nlPostV1506HeaderMapping.get(out_column)
        if pd.notnull(in_column):
            if in_column in indf.columns:
                outdf[out_column] = indf[in_column]
            else:
                outdf[out_column] = None

    # Identify the library name to search PID from
    library_name = outdf["LibraryEng"].ix[0]    # Assume whole dataframe use same library_name
    vod_library = vod_db[(vod_db["LIBRARY_NAME_ENG"] == library_name) & (vod_db['STATUS'] == 'ACTIVE')]
    if not any((vod_db["LIBRARY_NAME_ENG"] == library_name).tolist()):
       raise Exception("Library name cannot be found in database")
    
    # Construct full title in case it is not available in input file
    outdf = outdf.join(outdf.apply(dc.utils.constructFullTitle, axis=1))
    
    # Find most similar DB record for each input file record
    outdf[["ProductID_DB", "FullTitleEng_DB", "FullTitleChi_DB", "OpeningSlate_DB", "AudioSlate_DB", "AdvisorySlate_DB", "TVClassification_DB", "WebClassification_DB", "Score"]] = outdf.apply(find_similar_db_record, args=(vod_library,1,), axis=1)
    
    # Mark whether all the classification fields are matched for the input record and the picked DB record
    outdf["AllClassificationMatch"] = outdf.apply(lambda r: is_element_same(r["OpeningSlate_DB"], r["OpeningSlate"]) and is_element_same(r["AudioSlate_DB"], r["AudioSlate"]) and is_element_same(r["AdvisorySlate_DB"], r["AdvisorySlate"]) and is_element_same(r["TVClassification_DB"], r["TVClassification"]) and is_element_same(r["WebClassification_DB"], r["WebClassification"]), axis=1).map({True: "Y", False: "N"})
    
    # Mark whether the webclassification fields are matched for the input record and the picked DB record
    outdf["WebClassificationMatch"] = outdf.apply(lambda r: is_element_same(r["WebClassification_DB"], r["WebClassification"]) and (re.match("^1(\.0*){0,1}$",str(r["Score"])) is not None or str(r["Score"]) == '0.9'), axis=1).map({True: "Y", False: "N"})
    
    # Remove product id if not match web classification
    #outdf["ProductID_DB"] = outdf["ProductID_DB"][outdf["AllClassificationMatch"] == "Y"]
    outdf["ProductID_DB"] = outdf[["ProductID_DB", "WebClassificationMatch", "Score"]].apply(lambda x: "EXACT MATCH" if re.match("^1(\.0*){0,1}$",str(x["Score"])) is not None and x["WebClassificationMatch"] == "Y" else x["ProductID_DB"] if str(x["Score"]) == '0.9' and x["WebClassificationMatch"] == "Y" else None,axis=1)  
    
    # Drop columns that is no use
    outdf.drop([0, 1, "IsLive", "LibraryEng", "LibraryChi"], axis=1, inplace=True)

    # Write to output IO
#    writer = pd.ExcelWriter(out_io, engine='xlsxwriter')
#    outdf.to_excel(writer, index=False)
#    writer.save()

    # Write to output IO
#    indf2 = pd.read_excel(in_io, sheetname=0, keep_default_na=False)
    outdf2 = outdf[["ProductID_DB", "Score", "AllClassificationMatch", "WebClassificationMatch", ]].join(indf)
    # Convert PID to string
    outdf2['ProductID_DB'] = outdf2['ProductID_DB'].apply(lambda x: str(dc.utils.transferNumeric(x)) if pd.notnull(x) and str(x).strip() != '' and re.match("^[0-9]+$", str(dc.utils.transferNumeric(x))) is not None else x if str(x) == "EXACT MATCH" else None)
    # Convert date time to string
    outdf2['Start Date (YYYYMMDD)'] = outdf2['Start Date (YYYYMMDD)'].apply(lambda x: x if pd.isnull(x)
                                                                            else (str(int(x)) if re.fullmatch("[\d]{8}(\.0)*", str(x)) or re.fullmatch("[\d]{12}(\.0)*", str(x))
                                                                            else (x.strftime('%Y%m%d') if not isinstance(x, str)
                                                                            else x)))
    outdf2['End Date (YYYYMMDD)'] = outdf2['End Date (YYYYMMDD)'].apply(lambda x: x if pd.isnull(x)
                                                                            else (str(int(x)) if re.fullmatch("[\d]{8}(\.0)*", str(x)) or re.fullmatch("[\d]{12}(\.0)*", str(x))
                                                                            else (x.strftime('%Y%m%d') if not isinstance(x, str)
                                                                            else x)))
    outdf2['Duration (HH:MM:SS)'] = outdf2['Duration (HH:MM:SS)'].apply(lambda x: x if pd.isnull(x)
                                                                            else (str(x) if re.fullmatch("\d\d:\d\d:\d\d", str(x))
                                                                            else (x.strftime('%H:%M:%S') if not isinstance(x, str)
                                                                            else x)))
    writer = pd.ExcelWriter(out_io, engine='xlsxwriter')
    outdf2.to_excel(writer, index=False)
    writer.save()
