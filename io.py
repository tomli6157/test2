import pandas as pd
from io import BytesIO
import csv
import dc.utils
import dc.header

def to_io(out_df, out_io, out_format):
    if out_format == "preV1506":
        out_df.to_csv(out_io, encoding="big5-hkscs", float_format="%.0f", sep="\t", index=False, quoting=csv.QUOTE_NONE, columns=list(dc.header.preV1506HeaderMapping.keys()), header=list(dc.header.preV1506HeaderMapping.values()))
    elif out_format == "postV1506":
#        out_df.to_csv(out_io, encoding="big5-hkscs", float_format="%.0f", sep="\t", index=False, quoting=csv.QUOTE_NONE, columns=list(dc.header.postV1506HeaderMapping.keys()), header=list(dc.header.postV1506HeaderMapping.values()))
        sheet_name = "EPG"
        bio = BytesIO()
        writer = pd.ExcelWriter(bio, engine='xlsxwriter')
        out_df.to_excel(writer, sheet_name=sheet_name, index=False, columns=list(dc.header.postV1506HeaderMapping.keys()), header=list(dc.header.postV1506HeaderMapping.values()))
        writer.save()
        bio.seek(0)
        pd.read_excel(bio, sheetname=sheet_name, keep_default_na=False).to_excel(out_io, sheet_name=sheet_name, index=False)

    else:
        # Do nothing
        print("Error in writing DataFrame to file")

def to_io_nonlinear(out_df, out_io, out_format):
    if out_format == "postV1506":
        sheet_name = "VOD "
        bio = BytesIO()
        writer = pd.ExcelWriter(bio, engine='xlsxwriter')
        #out_df.to_excel(writer, sheet_name=sheet_name, index=False, columns=list(dc.header.nlPostV1506HeaderMapping.keys()), header=list(dc.header.nlPostV1506HeaderMapping.values()))
        out_df.to_excel(writer, sheet_name=sheet_name, index=False, columns=list(dc.utils.extendnlPOSTV1506(out_df.columns).keys()), header=list(dc.utils.extendnlPOSTV1506(out_df.columns).values()))
        writer.save()
        bio.seek(0)
        pd.read_excel(bio, sheetname=sheet_name, keep_default_na=False).to_excel(out_io, sheet_name=sheet_name, index=False)

    else:
        # Do nothing
        print("Error in writing DataFrame to file")
