from filename import generate_goes_url,generate_nexrad_url

# GOES FILENAMES
fileGOES1 = "OR_ABI-L1b-RadC-M6C01_G18_s20230020101172_e20230020103548_c20230020103594.nc"
fileGOES2 = "OR_ABI-L2-ACMM1-M6_G18_s20230090504262_e20230090504319_c20230090505026.nc"
fileGOES3 = "OR_ABI-L2-ACTPM1-M6_G18_s20230090408262_e20230090408319_c20230090409174.nc"
fileGOES4 = "OR_ABI-L2-DSIM1-M6_G18_s20230110608251_e20230110608308_c20230110609126.nc"
fileGOES5 = "OR_ABI-L2-ACHTM1-M6_G18_s20223560805242_e20223560805300_c20223560806526.nc"
fileGOES6 = "OR_ABI-L2-BRFF-M6_G18_s20223150230207_e20223150239515_c20223150241087.nc"
fileGOES7 = "OR_ABI-L2-ADPM2-M6_G18_s20230061310557_e20230061311015_c20230061311402.nc"
fileGOES8 = "OR_ABI-L1b-RadM1-M6C01_G18_s20230030201252_e20230030201311_c20230030201340.nc"
fileGOES9 = "OR_ABI-L2-ACHTF-M6_G18_s20223532240210_e20223532249518_c20223532252164.nc"
fileGOES10 = "OR_ABI-L2-DSRC-M6_G18_s20223180501179_e20223180503552_c20223180508262.nc"
fileGOES11 = "OR_ABI-L2-DMWVM1-M6C08_G18_s20223552050271_e20223552050328_c20223552122197.nc"
fileGOES12 = "OR_ABI-L2-ACMC-M6_G18_s20222800931164_e20222800933537_c20222800934574.nc"
fileGOES13 = "OR_ABI-L2-DMWC-M6C07_G18_s20223510516174_e20223510518559_c20223510527449.nc"

# GOES URLS (Example + Groups 1-12)
urlGOES1 = "https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadC/2023/002/01/OR_ABI-L1b-RadC-M6C01_G18_s20230020101172_e20230020103548_c20230020103594.nc"
urlGOES2 = "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACMM/2023/009/05/OR_ABI-L2-ACMM1-M6_G18_s20230090504262_e20230090504319_c20230090505026.nc"
urlGOES3 = "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACTPM/2023/009/04/OR_ABI-L2-ACTPM1-M6_G18_s20230090408262_e20230090408319_c20230090409174.nc"
urlGOES4 = "https://noaa-goes18.s3.amazonaws.com/ABI-L2-DSIM/2023/011/06/OR_ABI-L2-DSIM1-M6_G18_s20230110608251_e20230110608308_c20230110609126.nc"
urlGOES5 = "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACHTM/2022/356/08/OR_ABI-L2-ACHTM1-M6_G18_s20223560805242_e20223560805300_c20223560806526.nc"
urlGOES6 = "https://noaa-goes18.s3.amazonaws.com/ABI-L2-BRFF/2022/315/02/OR_ABI-L2-BRFF-M6_G18_s20223150230207_e20223150239515_c20223150241087.nc"
urlGOES7 = "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ADPM/2023/006/13/OR_ABI-L2-ADPM2-M6_G18_s20230061310557_e20230061311015_c20230061311402.nc"
urlGOES8 = "https://noaa-goes18.s3.amazonaws.com/ABI-L1b-RadM/2023/003/02/OR_ABI-L1b-RadM1-M6C01_G18_s20230030201252_e20230030201311_c20230030201340.nc"
urlGOES9 = "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACHTF/2022/353/22/OR_ABI-L2-ACHTF-M6_G18_s20223532240210_e20223532249518_c20223532252164.nc"
urlGOES10 = "https://noaa-goes18.s3.amazonaws.com/ABI-L2-DSRC/2022/318/05/OR_ABI-L2-DSRC-M6_G18_s20223180501179_e20223180503552_c20223180508262.nc"
urlGOES11 = "https://noaa-goes18.s3.amazonaws.com/ABI-L2-DMWVM/2022/355/20/OR_ABI-L2-DMWVM1-M6C08_G18_s20223552050271_e20223552050328_c20223552122197.nc"
urlGOES12 = "https://noaa-goes18.s3.amazonaws.com/ABI-L2-ACMC/2022/280/09/OR_ABI-L2-ACMC-M6_G18_s20222800931164_e20222800933537_c20222800934574.nc"
urlGOES13 = "https://noaa-goes18.s3.amazonaws.com/ABI-L2-DMWC/2022/351/05/OR_ABI-L2-DMWC-M6C07_G18_s20223510516174_e20223510518559_c20223510527449.nc"

#NEXRAD FILENAMES
fileNEXRAD1 = "KBGM20111010_000301_V03.gz"
fileNEXRAD2 = "KBGM20110612_003045_V03.gz"
fileNEXRAD3 = "KARX20100512_014240_V03.gz"
fileNEXRAD4 = "KBIS20001222_090728.gz"
fileNEXRAD5 = "KCCX20120203_013605_V03.gz"
fileNEXRAD6 = "KBYX20150804_000940_V06.gz"
fileNEXRAD7 = "KAPX20120717_013219_V06.gz"
fileNEXRAD8 = "KAPX20140907_010223_V06.gz"
fileNEXRAD9 = "KLWX19931112_005128.gz"


#NEXRAD URLS (Example + Groups 1,2,4,5,7,8,9,12 )
urlNEXRAD1 = "https://noaa-nexrad-level2.s3.amazonaws.com/2011/10/10/KBGM/KBGM20111010_000301_V03.gz"
urlNEXRAD2 = "https://noaa-nexrad-level2.s3.amazonaws.com/2011/06/12/KBGM/KBGM20110612_003045_V03.gz"
urlNEXRAD3 = "https://noaa-nexrad-level2.s3.amazonaws.com/2010/05/12/KARX/KARX20100512_014240_V03.gz"
urlNEXRAD4 = "https://noaa-nexrad-level2.s3.amazonaws.com/2000/12/22/KBIS/KBIS20001222_090728.gz"
urlNEXRAD5 = "https://noaa-nexrad-level2.s3.amazonaws.com/2012/02/03/KCCX/KCCX20120203_013605_V03.gz"
urlNEXRAD6 = "https://noaa-nexrad-level2.s3.amazonaws.com/2015/08/04/KBYX/KBYX20150804_000940_V06.gz"
urlNEXRAD7 = "https://noaa-nexrad-level2.s3.amazonaws.com/2012/07/17/KAPX/KAPX20120717_013219_V06.gz"
urlNEXRAD8 = "https://noaa-nexrad-level2.s3.amazonaws.com/2014/09/07/KAPX/KAPX20140907_010223_V06.gz"
urlNEXRAD9 = "https://noaa-nexrad-level2.s3.amazonaws.com/1993/11/12/KLWX/KLWX19931112_005128.gz"

#TESTING FUNCTIONS
def test_gen_goes_url():

    assert generate_goes_url(fileGOES1) == urlGOES1
    assert generate_goes_url(fileGOES2) == urlGOES2
    assert generate_goes_url(fileGOES3) == urlGOES3
    assert generate_goes_url(fileGOES4) == urlGOES4
    assert generate_goes_url(fileGOES5) == urlGOES5
    assert generate_goes_url(fileGOES6) == urlGOES6
    assert generate_goes_url(fileGOES7) == urlGOES7
    assert generate_goes_url(fileGOES8) == urlGOES8
    assert generate_goes_url(fileGOES9) == urlGOES9
    assert generate_goes_url(fileGOES10) == urlGOES10
    assert generate_goes_url(fileGOES11) == urlGOES11
    assert generate_goes_url(fileGOES12) == urlGOES12
    assert generate_goes_url(fileGOES13) == urlGOES13
    

def test_gen_nexrad_url():
    assert generate_nexrad_url(fileNEXRAD1) == urlNEXRAD1
    assert generate_nexrad_url(fileNEXRAD2) == urlNEXRAD2
    assert generate_nexrad_url(fileNEXRAD3) == urlNEXRAD3
    assert generate_nexrad_url(fileNEXRAD4) == urlNEXRAD4
    assert generate_nexrad_url(fileNEXRAD5) == urlNEXRAD5
    assert generate_nexrad_url(fileNEXRAD6) == urlNEXRAD6
    assert generate_nexrad_url(fileNEXRAD7) == urlNEXRAD7
    assert generate_nexrad_url(fileNEXRAD8) == urlNEXRAD8
    assert generate_nexrad_url(fileNEXRAD9) == urlNEXRAD9