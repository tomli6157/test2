from pandas.io.excel import ExcelFile
from warnings import warn
import numpy as np
import pandas.compat as compat
from pandas.compat import (string_types, OrderedDict)
from distutils.version import LooseVersion
from datetime import datetime, date, time, MINYEAR
from pandas.compat import (lrange)
from pandas.io.common import (_is_url, _urlopen, _validate_header_arg,
                              EmptyDataError, get_filepath_or_buffer,
                              _NA_VALUES)
from pandas.io.parsers import TextParser
from pandas.core.frame import DataFrame
from pandas.api.types import (is_integer, is_float,
                                 is_bool, is_list_like)

def read_excel(io, sheetname=0, header=0, skiprows=None, skip_footer=0,
               index_col=None, names=None, parse_cols=None, parse_dates=False,
               date_parser=None, na_values=None, thousands=None,
               convert_float=True, has_index_names=None, converters=None,
               dtype=None, true_values=None, false_values=None, engine=None,
               squeeze=False, **kwds):

    if not isinstance(io, test1):
        io = test1(io, engine=engine)

    return io._parse_excel(
        sheetname=sheetname, header=header, skiprows=skiprows, names=names,
        index_col=index_col, parse_cols=parse_cols, parse_dates=parse_dates,
        date_parser=date_parser, na_values=na_values, thousands=thousands,
        convert_float=convert_float, has_index_names=has_index_names,
        skip_footer=skip_footer, converters=converters, dtype=dtype,
        true_values=true_values, false_values=false_values, squeeze=squeeze, 
        **kwds)

class test1(ExcelFile):
    def _parse_excel(self, sheetname=0, header=0, skiprows=None, names=None,
                     skip_footer=0, index_col=None, has_index_names=None,
                     parse_cols=None, parse_dates=False, date_parser=None,
                     na_values=None, thousands=None, convert_float=True,
                     true_values=None, false_values=None, verbose=False,
                     dtype=None, squeeze=False, **kwds):
        
        skipfooter = kwds.pop('skipfooter', None)
        if skipfooter is not None:
            skip_footer = skipfooter

        _validate_header_arg(header)
        if has_index_names is not None:
            warn("\nThe has_index_names argument is deprecated; index names "
                 "will be automatically inferred based on index_col.\n"
                 "This argmument is still necessary if reading Excel output "
                 "from 0.16.2 or prior with index names.", FutureWarning,
                 stacklevel=3)

        if 'chunksize' in kwds:
            raise NotImplementedError("chunksize keyword of read_excel "
                                      "is not implemented")

        if parse_dates is True and index_col is None:
            warn("The 'parse_dates=True' keyword of read_excel was provided"
                 " without an 'index_col' keyword value.")
        
        def _parse_cell(cell_contents, cell_typ):
            """converts the contents of the cell into a pandas
               appropriate object"""
            
            if cell_typ == XL_CELL_DATE:

                if xlrd_0_9_3:
                    # Use the newer xlrd datetime handling.
                    try:
                        cell_contents = \
                            xldate.xldate_as_datetime(cell_contents,
                                                      epoch1904)
                    except OverflowError:
                        return cell_contents
                    # Excel doesn't distinguish between dates and time,
                    # so we treat dates on the epoch as times only.
                    # Also, Excel supports 1900 and 1904 epochs.
                    year = (cell_contents.timetuple())[0:3]
                    if ((not epoch1904 and year == (1899, 12, 31)) or
                            (epoch1904 and year == (1904, 1, 1))):
                        cell_contents = time(cell_contents.hour,
                                             cell_contents.minute,
                                             cell_contents.second,
                                             cell_contents.microsecond)
                else:
                    # Use the xlrd <= 0.9.2 date handling.
                    try:
                        dt = xldate.xldate_as_tuple(cell_contents, epoch1904)

                    except xldate.XLDateTooLarge:
                        return cell_contents

                    if dt[0] < MINYEAR:
                        cell_contents = time(*dt[3:])
                    else:
                        cell_contents = datetime(*dt)

            elif cell_typ == XL_CELL_ERROR:
                cell_contents = np.nan
            elif cell_typ == XL_CELL_BOOLEAN:
                cell_contents = bool(cell_contents)
            elif convert_float and cell_typ == XL_CELL_NUMBER:
                # GH5394 - Excel 'numbers' are always floats
                # it's a minimal perf hit and less suprising
                val = int(cell_contents)
                if val == cell_contents:
                    cell_contents = val
            return cell_contents
        
        ret_dict = False
        if isinstance(sheetname, list):
            sheets = sheetname
            ret_dict = True
        elif sheetname is None:
            sheets = self.sheet_names
            ret_dict = True
        else:
            sheets = [sheetname]

        # handle same-type duplicates.
        sheets = list(OrderedDict.fromkeys(sheets).keys())
        output = OrderedDict()
        
        import xlrd
        from xlrd import (xldate, XL_CELL_DATE,
                          XL_CELL_ERROR, XL_CELL_BOOLEAN,
                          XL_CELL_NUMBER)
        
        epoch1904 = self.book.datemode
        
        # xlrd >= 0.9.3 can return datetime objects directly.
        if LooseVersion(xlrd.__VERSION__) >= LooseVersion("0.9.3"):
            xlrd_0_9_3 = True
        else:
            xlrd_0_9_3 = False
        
        # Keep sheetname to maintain backwards compatibility.
        for asheetname in sheets:
            if verbose:
                print("Reading sheet %s" % asheetname)
            if isinstance(asheetname, compat.string_types):
                sheet = self.book.sheet_by_name(asheetname)
            else:  # assume an integer if not a string
                sheet = self.book.sheet_by_index(asheetname)

            data = []
            should_parse = {}

            if sheet.nrows > 5000:
                raise Exception("The raw file contains more than 5000 rows. Please check if it is correct or split the files (max: 5000 rows) for upload")
            elif kwds.get('MaxTest'):
                continue

            for i in range(sheet.nrows):
                
                row = []
                for j, (value, typ) in enumerate(zip(sheet.row_values(i),
                                                     sheet.row_types(i))):
                    if parse_cols is not None and j not in should_parse:
                        should_parse[j] = self._should_parse(j, parse_cols)

                    if parse_cols is None or should_parse[j]:
                        row.append(_parse_cell(value, typ))
                data.append(row)
#            output[asheetname] = data
            if sheet.nrows == 0:
                output[asheetname] = DataFrame()
                continue

            if is_list_like(header) and len(header) == 1:
                header = header[0]
            
            # forward fill and pull out names for MultiIndex column
            header_names = None
            if header is not None:
                if is_list_like(header):
                    header_names = []
                    control_row = [True for x in data[0]]
                    for row in header:
                        if is_integer(skiprows):
                            row += skiprows

                        data[row], control_row = _fill_mi_header(
                            data[row], control_row)
                        header_name, data[row] = _pop_header_name(
                            data[row], index_col)
                        header_names.append(header_name)
            
            if is_list_like(index_col):
                # forward fill values for MultiIndex index
                if not is_list_like(header):
                    offset = 1 + header
                else:
                    offset = 1 + max(header)

                for col in index_col:
                    last = data[offset][col]
                    for row in range(offset + 1, len(data)):
                        if data[row][col] == '' or data[row][col] is None:
                            data[row][col] = last
                        else:
                            last = data[row][col]

            if is_list_like(header) and len(header) > 1:
                has_index_names = True
                
            if kwds.get('parsed'):
                try:
                    parser = TextParser(data, header=header, index_col=index_col,
                                    has_index_names=has_index_names,
                                    na_values=na_values,
                                    thousands=thousands,
                                    parse_dates=parse_dates,
                                    date_parser=date_parser,
                                    true_values=true_values,
                                    false_values=false_values,
                                    skiprows=skiprows,
                                    skipfooter=skip_footer,
                                    squeeze=squeeze,
                                    dtype=dtype,
                                    **kwds)
                    output[asheetname] = parser.read()
                    if names is not None:
                        output[asheetname].columns = names
                    if not squeeze or isinstance(output[asheetname], DataFrame):
                        output[asheetname].columns = output[
                            asheetname].columns.set_names(header_names)
                except EmptyDataError:
                    # No Data, return an empty DataFrame
                    output[asheetname] = DataFrame()
            else:
                output[asheetname] = data

        if ret_dict or kwds.get('MaxTest'):
            return output
        else:
            return output[asheetname]
