from fastapi import FastAPI
from pydantic import BaseModel

app =FastAPI()

class UserInput(BaseModel):
    year:str
    month:str
    date:str
    station:str

@app.get("/say_hello")  #endpoint name
async def say_hello() -> dict:
    return {"message":"Hello World"}


@app.post("/fetch_url") #endpoint name
async def fetch_url(userinput: UserInput) -> dict:
    # if userinput.date > 31:
    #     return 400 bad request . return incorrect date
    aws_nexrad_url = f"https://noaa-nexrad-level2.s3.amazonaws.com/index.html#{userinput.year}/{userinput.month}/{userinput.date}/{userinput.station}"
    return {'url': aws_nexrad_url }