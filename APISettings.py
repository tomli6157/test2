from collections import OrderedDict
from dc.cpAPI import *

cleanser = {
    "linear" : OrderedDict([
        ("ViuTV6", ViuTV.convert_linear), # 96
        ("ViuTV", ViuTV.convert_linear), # 99
        ("now102", now102.convert_linear), # 102
        ("HRC", now102.convert_linear), # 105
        ("Jelli", now102.convert_linear), # 108
        ("Now Premier League 1", beINNowSports2.convert_linear), # 621
        ("Now Sports Prime", beINNowSports2.convert_linear), # 630
        ("beIN – Now Sports 2", beINNowSports2.convert_linear), # 632
        ])
}
