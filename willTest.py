import requests
import time
import numpy as np
import csv

DIRECTORY_NAME = "data/"


AUTH_TOKENS = [
    "03AKH6MRGfsxcnXXHz42b-rVmvIdqyihro1nLGOC7M7__kVGYhi_RrRWbmly-9Gw9WjVJBGcC56pO9phaU2J4N4d19wcdWMwEoMbsOUtjwOBAd5zMNvCgJ3lqCSBD5TjUQbceWZ9_NEhzPhdcvOwCQX43oWccNJlb8K-cONdhLUfz3ATAIo1DwJJ7xJ1-8zBWXEk8KSUV_kJv8Dc7X0jXHP-6stRkBqCy7vb_2N6iaBkZiN6arjmAv_dLAuEAociG-omwAHY-SObhs7q2SBGRHRs1FEW-J5CmEN6YSavK5XLH8TbfJ8pwhjNisNkk6B9eZTOSrvUj5FFVsdpm_JRz93-dm-RCKVuYY7vU32kitQpvLXHIJAEqttQKFBwmuo8I0DNDMpHhREUGfamPnB5a0aCeVzy4P7aLt4iSuZgCNKv-9mPMY7lYgvdBHxohxzKipY5izVfnTNLuHGPyEuFndiDhWcqv5LVt6HItwJibkWnOFiTStqW9kBSzjmkbBiuogzvZPdThH4LHxhqWm4i4LNNwnjfPe3ndyDw",
    "03AKH6MREVzaO5dbQnPk8EBNnsrA2f0oo9adZdp7d8Td3mHVUeFjWyyPEV10gwVK35RLN_F1HcK1yxp9x_TeFTeKh_l70QupyCLasw-jpyp2ggp172Q1Ky-oJvClOKJHaWd7k_qoPuNXkR4J7KbiRypgpafY0K19_g3TH8dGmvjukn8V4SAxSfCO58BDfFhIz-TbQCXx-Cw63MmnAt9QhjY9Vx9xY5KRVCdSpdovUjE77coT_9pbohALg_2gpwAJ9pttPg_UNqtaDAW9MGCFiLaFX5D-FCXNRsJ06_rVM8u6TPPdLhBTRhpC-d1BoG8ToEDCTERtxs3tj-sYW5TuuUsDmhKCDWyL9v41JGrE5duAQdZ6eX980cf6UZgj4m8SYPiE8yy3wgV42LM51YKA27KwIV_86qKNBSm1DqPDG_HvpZEUnMLzt9XVrknRyoXpfxV_rMxkeuhFDIFRaKmV5xIU6OhwUTcCjEHFn6OsReWFpZyLF7jbS66Ezap2IyZblebPZRMAXivGpvkBoqCO8Km3G41AbceyKz6g",
    "03AKH6MRGko8aHTZJr7d8VXTYsXf14ANRGCl7nv4yo2rviEzYvuvg63NYdbRBppj4hIiU_dDXpEbZJ5e57bDRgcw7VctOqnK_o6Q3v3eCFUvUea_76NE0DJLGnsByinwH7ot0b7mlbFN_mRo5I1cUf6_Li-s5Y1uHJPLrQlq27dhu-B_H3KAon2zFV8nwUKYBRfA6B55WIma6A-efHq9cUJCpz2SsRY4fx0iPDapKz1S3pXb6Lg-GR9etaGr24XDJzqunOLLonT6289fSR6PDmnOzlxfYuiE-kkh9nhrD39DFV7-nhzb1yOqqPeSkoCIFX9-Eehgbl4jT181AFS-uK_xr6gy-NcsAlUYqtzBAWyojg_bQQ5c_NUGdI2j5qg1AipBaZwH1Hy_C8vyh99ALNx0I2P_iL4k4GbiamhlTrYiRujBE679A-Y3ioHbdBSRvThO0Sj2bc7ftiusCmO-ffbV_o8OaLK6Vdk6HF6SMCA3Zalh-JZSu9av7WKC1LeTcM1HoKcOctamsLT4gr9i9N87j1Tmi0Db1ZD6EQAH_AZ7Elo9OkVe7TW10"
]

SLEEP_T = 2

# helper functions for making queries
postQueryEndpoint = "https://data.stackexchange.com/query/save/1"
getQueryEndpoint = "https://data.stackexchange.com/query/job/jobId"


def saveAs(fileName, content):
    # Writing to sample.json
    with open(fileName, "w") as outfile:
        outfile.write(content)

global lastAuthToken
lastAuthToken = 0
def getAuthToken():
    global lastAuthToken
    lastAuthToken = (lastAuthToken + 1) % len(AUTH_TOKENS)
    return AUTH_TOKENS[lastAuthToken]

def makeQuery(query, fileName=None):
    query = {
        "sql": query,
        "g-recaptcha-response": getAuthToken()
    }

    # create initial query
    response = requests.post(postQueryEndpoint, data = query)
    response = response.json()

    running = response.get("running")
    getEndpoint = getQueryEndpoint

    # if this is a larger query, modify the get endpoint
    if running:
        getEndpoint = getEndpoint.replace("jobId", response.get("job_id"))

    # continue checking every interval for results
    while running:
        time.sleep(SLEEP_T)
        response = requests.get(getEndpoint, params = {"_": time.time()}).json()
        running = response.get("running")
    
    # save content to json if fileName provided
    content = response.get("resultSets")[0]
    if fileName: saveAs(fileName, content)

    return content


# making queries


def constructColumns(columns):
    columnNames = []
    for column in columns:
        columnNames.append(column.get("name"))
    return ','.join(columnNames) + '\n'


NUM_USERS = 2500
INCREMENT = 125

# code used to retrieve random ids
# import random
# random.seed("BOOBIES")

# ids = set()

# def createUsers():
#     print("starting query")
#     response = makeQuery("""
#     SELECT DISTINCT UserId FROM PostHistory
#     """).get("rows")[1:]
#     print("cumming")

#     size = len(response)

#     while len(ids) < NUM_USERS:
#         id = response[random.randint(0, size)][0]
#         if id == None or id <= 0: continue
#         ids.add(id)

# createUsers()
# ids = list(ids)

import json

FILE_NAME = "data/UserIds.json"

# write all userids
# with open(FILE_NAME, 'w') as f:
#     json.dump(ids, f)

# load all usersids
with open(FILE_NAME, 'r') as f:
    ids = json.load(f)

content = None

with open('Posts.csv', 'w', newline='') as csvfile:
    # Create a CSV writer object
    writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

    for i in range(1, int(NUM_USERS/INCREMENT + 1)):
        low = INCREMENT * (i - 1)
        high = INCREMENT * i

        print("iteration: ", i)
        userIds = ids[low:high]        

        query = f""" 
        SELECT * FROM PostHistory WHERE UserId IN ({', '.join(map(str, userIds))});
        """
        print(query)
        response = makeQuery(query)

        print("done")
        if content == None:
            columns = []
            for column in response.get("columns"):
                columns.append(column.get("name"))

            # Write the header row
            writer.writerow(columns)

        # Loop through the JSON data and write each row to the CSV file
        for row in response.get("rows"):
            writer.writerow(row)

        print(len(response.get("rows")))

# with open('data/Users.csv', 'w', newline='') as csvfile:
#     # Create a CSV writer object
#     writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

#     query = f""" 
#     SELECT * FROM Users WHERE Id IN ({', '.join(map(str, ids))});
#     """
#     print(query)
#     response = makeQuery(query)

#     columns = []
#     for column in response.get("columns"):
#         columns.append(column.get("name"))

#     # Write the header row
#     writer.writerow(columns)

#     # Loop through the JSON data and write each row to the CSV file
#     for row in response.get("rows"):
#         writer.writerow(row)

#     print(len(response.get("rows")))

# with open('data/Badges.csv', 'w', newline='') as csvfile:
#     # Create a CSV writer object
#     writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

#     query = f""" 
#     SELECT * FROM Badges WHERE UserId IN ({', '.join(map(str, ids))});
#     """
#     print(query)
#     response = makeQuery(query)

#     columns = []
#     for column in response.get("columns"):
#         columns.append(column.get("name"))

#     # Write the header row
#     writer.writerow(columns)

#     # Loop through the JSON data and write each row to the CSV file
#     for row in response.get("rows"):
#         writer.writerow(row)

#     print(len(response.get("rows")))


# with open('data/Comments.csv', 'w', newline='') as csvfile:
#     # Create a CSV writer object
#     writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

#     query = f""" 
#     SELECT * FROM Comments WHERE UserId IN ({', '.join(map(str, ids))});
#     """
#     print(query)
#     response = makeQuery(query)

#     columns = []
#     for column in response.get("columns"):
#         columns.append(column.get("name"))

#     # Write the header row
#     writer.writerow(columns)

#     # Loop through the JSON data and write each row to the CSV file
#     for row in response.get("rows"):
#         writer.writerow(row)

#     print(len(response.get("rows")))