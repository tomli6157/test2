from collections import OrderedDict
from dc.cp import *


char_limit = {
    "BrandNameEng" : 200,
    "BrandNameChi" : 200,
    "SeasonNameEng" : 50,
    "SeasonNameChi" : 50,
    "EpisodeNameEng" : 75,
    "EpisodeNameChi" : 75,
    "SynopsisEng" : 990,
    "SynopsisChi" : 990,
    "ShortSynopsisEng" : 120,
    "ShortSynopsisChi" : 120,
    "SubGenre" : 100,
    "DirectorProducerEng" : 255,
    "DirectorProducerChi" : 255,
    "CastEng" : 255,
    "CastChi" : 255
}

cleanser = {
    "linear" : OrderedDict([
        ("ViuTVsix", ViuTV.convert_linear), # 96
        ("ViuTV", ViuTV.convert_linear), # 99
        ("Now Drama Channel",NowDramaChannel.convert_linear), # 102
        ("Now Chinese Drama Channel",NowDramaChannel.convert_linear), # 105
        ("Now Video Express",NowVideoExpress.convert_linear), # 106
        ("NowJelli",NowDramaChannel.convert_linear), # 108
        ("HBO HD", HBO.convert_linear), # 110
        ("HBO Hits", HBO.convert_linear), # 111
        ("HBO Family", HBO.convert_linear), # 112
        ("CINEMAX", HBO.convert_linear), # 113
        ("HBO Signature", HBO.convert_linear), # 114
        ("HBO", HBO.convert_linear), # 115
        ("MOVIE MOVIE", MOVIEMOVIE.convert_linear), # 116
        ("FOX Movies", FOXMoviesPremium.convert_linear), # 117
        ("FOX Action Movies", FOXActionMovies.convert_linear), # 118
        ("FOX Family Movies", FOXFamilyMovies.convert_linear), # 120
        ("Thrill", Thrill.convert_linear), # 125
        ("Turner Classic Movies", TurnerClassicMovies.convert_linear), # 126
        ("Now Baogu Movies", NowBaoguMovies.convert_linear), # 133
        ("SCM", SCM.convert_linear), # 139
        ("SCM Legend", SCMLegend.convert_linear), # 140
        ("Animax", Animax.convert_linear), # 150
        ("GEM", GEM.convert_linear), # 151
        ("Oh!K", OhK.convert_linear), # 154
        ("tvN", tvN.convert_linear), # 155
        ("KBS World", KBSWorld.convert_linear), # 156
        ("Star Chinese Channel", StarChineseChannel.convert_linear), # 160
        ("ETTV Asia Channel", ETTVAsiaChannel.convert_linear), # 162
        ("TVBS Asia", TVBSAsia.convert_linear), # 163
        ("Discovery Asia", DiscoveryScience.convert_linear), # 208
        ("Discovery Channel", DiscoveryScience.convert_linear), # 209
        ("Animal Planet", DiscoveryScience.convert_linear), # 210
        ("Discovery Science", DiscoveryScience.convert_linear), # 211
        ("DMAX", DiscoveryScience.convert_linear), # 212
        ("TLC", DiscoveryScience.convert_linear), # 213
        ("EVE", DiscoveryScience.convert_linear), # 214
        ("National Geographic", NationalGeographic.convert_linear), # 215
        ("Nat Geo Wild", NationalGeographic.convert_linear), # 216
        ("Nat Geo People", NationalGeographic.convert_linear), # 217
        ("National Geographic HD", NationalGeographic.convert_linear), # 218
        ("BBC Earth", BBCEarth.convert_linear), # 220
        ("FYI", FYI.convert_linear), # 222
        ("HISTORY", FYI.convert_linear), # 223
        ("HISTORY HD", FYI.convert_linear), # 225
        ("Crime & Investigation Network", FYI.convert_linear), # 226
        ("CNN International", CNNInternational.convert_linear), # 316
        ("HLN", HLN.convert_linear), # 317
        ("Fox News", FoxNews.convert_linear), # 318
        ("CNBC", CNBC.convert_linear), # 319
        ("BBC World News", BBCWorldNews.convert_linear), # 320
        ("Bloomberg Television", BloombergTelevision.convert_linear), # 321
        ("Channel NewsAsia", ChannelNewsAsia.convert_linear), # 322
        ("Sky News", SkyNews.convert_linear), # 323
        ("DW (English)", DWDeutsch.convert_linear), # 324
        ("Al Jazeera English", AlJazeeraEnglish.convert_linear), # 325
        ("euronews", euronews.convert_linear), # 326
        ("France 24", CCTV4.convert_linear), # 327
        ("NHK WORLD-JAPAN", NHKWORLDJAPAN.convert_linear), # 328
        ("RT", RT.convert_linear), # 329
        ("Now Direct", NowBusinessNewsChannel.convert_linear), # 331
        ("Now NEWS", NowBusinessNewsChannel.convert_linear), # 332
        ("Now Business News Channel", NowBusinessNewsChannel.convert_linear), # 333
        ("Yicai TV", ChinaBusinessNetwork.convert_linear), # 338
        ("Phoenix InfoNews Channel", PhoenixInfoNewsChannel.convert_linear), # 366
        ("Phoenix Hong Kong Channel", PhoenixHongKongChannel.convert_linear), # 367
        ("HKSTV Zhonghe", HKSTVZhonghe.convert_linear), # 368
        ("CNC Chinese", CNCChinese.convert_linear), # 369
        ("CNC World", CNCWorld.convert_linear), # 370
        ("ETTV Asia News", ETTVAsiaNews.convert_linear), # 371
        ("Traffic Channel", TrafficChannel.convert_linear), # 375
        ("Weather Channel", TrafficChannel.convert_linear), # 376
        ("Disney Channel", DisneyChannel.convert_linear), # 441
        ("Disney Junior", DisneyJunior.convert_linear), # 442
        ("Cartoon Network", CartoonNetwork.convert_linear), # 443
        ("Nickelodeon", Nickelodeon.convert_linear), # 444
        ("Boomerang", Boomerang.convert_linear), # 445
        ("CBeebies", BBCEarth.convert_linear), # 447
        ("Baby TV", BabyTV.convert_linear), # 448
        ("Nick Jr.", NickJr.convert_linear), # 449
        ("BBC Lifestyle", BBCEarth.convert_linear), # 502
        ("E!", E.convert_linear), # 506
        ("DIVA", DIVA.convert_linear), # 508
        ("WarnerTV", WarnerTV.convert_linear), # 510
        ("AXN", AXN.convert_linear), # 512
        ("Sony Channel", SonyChannel.convert_linear), # 514
        ("BLUE ANT Entertainment", BLUEANTEntertainment.convert_linear), # 517
        ("FOX", FOX.convert_linear), # 518
        ("FOXlife", FOXlife.convert_linear), # 521
        ("FOXCRIME", FOXlife.convert_linear), # 523
        ("FX", FX.convert_linear), # 524
        ("Lifetime", FYI.convert_linear), # 525
        ("Food Network", FoodNetwork.convert_linear), # 526
        ("Asian Food Channel", AsianFoodChannel.convert_linear), # 527
        ("[V] International", VInternational.convert_linear), # 534
        ("Pearl River Channel", PearlRiverChannel.convert_linear), # 537
        ("CTI Asia Channel", CTIAsiaChannel.convert_linear), # 538
        ("Dim Sum TV", DimSumTV.convert_linear), # 539
        ("Shenzhen TV", ShenzhenTV.convert_linear), # 540
        ("CCTV-1", CCTV1.convert_linear), # 541
        ("CCTV- 4", CCTV4.convert_linear), # 542
        ("Southern Television", SouthernTelevision.convert_linear), # 543
        ("MASTV", MASTV.convert_linear), # 544
        ("Creation TV", CreationTV.convert_linear), # 545
        ("Phoenix Chinese Channel", PhoenixChineseChannel.convert_linear), # 548
        ("Da Ai", DaAi.convert_linear), # 549
        ("Taoist TV", TaoistTV.convert_linear), # 550
        ("One TV", OneTV.convert_linear), # 552
        ("Zhejiang Satellite TV", ZhejiangSatelliteTV.convert_linear), # 555
        ("China Chinese Satellite TV", ChinaChineseSatelliteTV.convert_linear), # 556
        ("ABC Australia", ABCAustralia.convert_linear), # 561
        ("Premier League TV", NowPremierLeague2.convert_linear), # 620
        ("Now Premier League 1", beINNowSports2.convert_linear), # 621
        ("Now Premier League 2", NowPremierLeague2.convert_linear), # 622
        ("Now Premier League 3", NowPremierLeague2.convert_linear), # 623
        ("Now Premier League 4", NowPremierLeague2.convert_linear), # 624
        ("Now Premier League 5", NowPremierLeague2.convert_linear), # 625
        ("Now Premier League 6", NowPremierLeague2.convert_linear), # 626
        ("Now Sports Prime", beINNowSports2.convert_linear), # 630
        ("Now Sports 1", beINNowSports2.convert_linear), # 631
        ("beIN – Now Sports 2", beINNowSports2.convert_linear), # 632
        ("Now Sports 3", beINNowSports2.convert_linear), # 633
        ("Now Sports 4", beINNowSports2.convert_linear), # 634
        ("Now Sports 5", beINNowSports2.convert_linear), # 635
        ("Now Sports 6", beINNowSports2.convert_linear), # 636
        ("Now Sports 7", beINNowSports2.convert_linear), # 637
        ("beIN SPORTS 1", NowPremierLeague2.convert_linear), # 638
        ("beIN SPORTS 2", NowPremierLeague2.convert_linear), # 639
        ("MUTV", MUTV.convert_linear), # 640
        ("FIGHT SPORTS", FIGHTSPORTS.convert_linear), # 642
        ("beIN SPORTS Max", NowPremierLeague2.convert_linear), # 643
        ("beIN SPORTS Max 2", NowPremierLeague2.convert_linear), # 644
        ("beIN SPORTS Max 3", NowPremierLeague2.convert_linear), # 645
        ("Now668", beINNowSports2.convert_linear), # 668
        ("FOX SPORTS", FOXSPORTS3.convert_linear), # 670
        ("FOX SPORTS 2", FOXSPORTS3.convert_linear), # 671
        ("FOX SPORTS 3", FOXSPORTS3.convert_linear), # 672
        ("STAR Cricket", STARCricket.convert_linear), # 674
        ("Setanta Sports Channel", SetantaSportsChannel.convert_linear), # 679
        ("Now Sports 680", beINNowSports2.convert_linear), # 680
        ("Now Golf 1", beINNowSports2.convert_linear), # 682
        ("Now Golf 2", beINNowSports2.convert_linear), # 683
        ("Now Golf 3", beINNowSports2.convert_linear), # 684
        ("NHK World Premium", NHKWorldPremium.convert_linear), # 711
        ("TV5MONDE Style", TV5MONDEASIE.convert_linear), # 713
        ("TV5MONDE ASIE", TV5MONDEASIE.convert_linear), # 714
        ("France 24 (French)", CCTV4.convert_linear), # 715
        ("GMA Pinoy TV", GMAPinoyTV.convert_linear), # 720
        ("GMA Life TV", GMAPinoyTV.convert_linear), # 721
        ("GMA News TV International", GMAPinoyTV.convert_linear), # 722
        ("TFC", TFC.convert_linear), # 725
        ("DW Deutsch", DWDeutsch.convert_linear), # 765
        ("Sony TV (India)", SonyTVIndia.convert_linear), # 771
        ("Sony MAX", SonyTVIndia.convert_linear), # 772
        ("Sony MIX", SonyTVIndia.convert_linear), # 773
        ("Sony SAB", SonyTVIndia.convert_linear), # 774
        ("MTV India", MTVIndia.convert_linear), # 779
        ("COLORS", COLORS.convert_linear), # 780
        ("Zee Cinema International", ZeeCinemaInternational.convert_linear), # 781
        ("Zee TV", ZeeTV.convert_linear), # 782
        ("Zee News", ZeeNews.convert_linear), # 785
        ("Star Gold", StarGold.convert_linear), # 793
        ("STAR PLUS", STARPLUS.convert_linear), # 794
        ("Star Bharat", STARPLUS.convert_linear), # 797
        ("Variety and Travel", VarietyandTravel.convert_linear), # 801
        ("Food", VarietyandTravel.convert_linear), # 803
        ("Entertainment News", VarietyandTravel.convert_linear), # 804
        ("TVBN", VarietyandTravel.convert_linear), # 805
        ("Japanese Drama", VarietyandTravel.convert_linear), # 807
        ("Korean Drama", VarietyandTravel.convert_linear), # 808
        ("TVB Classic", VarietyandTravel.convert_linear), # 809
        ("Chinese Drama", VarietyandTravel.convert_linear), # 810
        ("Asian Select", VarietyandTravel.convert_linear), # 811
        ("Jade Catch Up", VarietyandTravel.convert_linear), # 816
        ("Classic Movies", VarietyandTravel.convert_linear), # 817
        ("Ice Fire", IceFire.convert_linear), # 901
        ("KiMoChi Channel", KiMoChiChannel.convert_linear), # 902
        ("Channel Adult", ChannelAdult.convert_linear), # 903
        ]),
    "nonlinear" : OrderedDict([
        ("FOX", FOX.convert_nonlinear), # FOX
        ("Discovery", Discovery.convert_nonlinear), # Discovery
        ("HBO", HBO.convert_nonlinear), # HBO
        ("NBCU", NBCU.convert_nonlinear), # NBCU
        ("Oh!K", OhK.convert_nonlinear), # Oh!K
        ])
}
