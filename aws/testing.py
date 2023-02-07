from filename import generate_goes_url, generate_nexrad_url
import pandas as pd
#import filename

goes_test_cases = pd.read_csv('GOES test cases.csv')
goes_test_cases = goes_test_cases.iloc[:-3]     #the last 3 rows of the sheet shared in class are not test cases
goes_filenames = goes_test_cases['File name'].str.strip().to_list()
goes_output_url = goes_test_cases['Full file name'].str.strip().to_list()

def test_generate_goes_url_01():
    assert generate_goes_url(goes_filenames[0]) == goes_output_url[0]

def test_generate_goes_url_02():
    assert generate_goes_url(goes_filenames[1]) == goes_output_url[1]

def test_generate_goes_url_03():
    assert generate_goes_url(goes_filenames[2]) == goes_output_url[2]

def test_generate_goes_url_04():
    assert generate_goes_url(goes_filenames[3]) == goes_output_url[3]

def test_generate_goes_url_05():
    assert generate_goes_url(goes_filenames[4]) == goes_output_url[4]

def test_generate_goes_url_06():
    assert generate_goes_url(goes_filenames[5]) == goes_output_url[5]

def test_generate_goes_url_07():
    assert generate_goes_url(goes_filenames[6]) == goes_output_url[6]

def test_generate_goes_url_08():
    assert generate_goes_url(goes_filenames[7]) == goes_output_url[7]

def test_generate_goes_url_09():
    assert generate_goes_url(goes_filenames[8]) == goes_output_url[8]

def test_generate_goes_url_10():
    assert generate_goes_url(goes_filenames[9]) == goes_output_url[9]

def test_generate_goes_url_11():
    assert generate_goes_url(goes_filenames[10]) == goes_output_url[10]

def test_generate_goes_url_12():
    assert generate_goes_url(goes_filenames[11]) == goes_output_url[11]

def test_generate_goes_url_13():
    assert generate_goes_url(goes_filenames[12]) == goes_output_url[12]