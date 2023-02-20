def say_hello():
    return "Hello World"

def fetch_url(
    year: str,
    month: str,
    date: str,
    station: str
):
    aws_nexrad_url = f"https://noaa-nexrad-level2.s3.amazonaws.com/index.html#{year}/{month}/{date}/{station}"
    return aws_nexrad_url

print(say_hello())
func_op = fetch_url('2022','06','21','KAMX')
print(func_op)