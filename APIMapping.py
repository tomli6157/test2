from collections import OrderedDict

channelMapping = {"96": "ViuTV6",
                   "99": "ViuTV",
                   "102": "now102",
                   "105": "HRC",
                   "108": "Jelli",
                   "621": "Now Premier League 1",
                   "630": "Now Sports Prime",
                   "632": "beIN – Now Sports 2",
                      }    
ownerChannelMapping = {}
for channelNo, info in channelMapping.items():
    ownerChannelMapping[info] = channelNoasfaf