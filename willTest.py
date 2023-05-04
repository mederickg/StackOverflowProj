import requests
import time
import numpy as np
import csv

DIRECTORY_NAME = "data/"


AUTH_TOKENS = [
    "03AL8dmw-5OAYOeeyDZusZopl0ZOemWKxs5Z5lVXJKpShY1qKIIsBV_tyipuJK9IPdK6c5QesaCmL3OnvVQAwnOJLSdZIlkXQqKNA2pHTVI8g_GGHaMJCBIDdE3rqNUEDWp24wyCqMRS-VYidD9RBVuTIyrgxeiznKwoIzRuz0lkWo_2ravC0paMhL44gCbQ5JiukZzUnlJwhCL6ZIC0ziHxzs84cNLd12hjR3xPOGSqG4KqltxRQsmu6xwe_XGGRwoGuvssOcCHAItcznIp2p9jG_-E-oQjnFOh10rQRZzLv7IcCrc95F3ecqT5FRfj1V-dGgDLoFMqMyx2MAEeg2rBXM1ke5YkDP5ShJx666kCmZzAlYWyC7jk8zIjyqFIha7Q9qY2rpEnwo3sTxVXYd6Nm6npAukzcZMZtMaYnbKjcaAe8SnyWVGhbzeOnVex8GwTHRS5wBhGjlsNoHMyICf778POaxe-v1J_3B0IeePQ1ja_6DygA-Xt34Noeu-1RCzREDJpQHGN30riftm5Nxg2xsoOn71C-pAAGf-itm1FkmqSM1IrguECQjgir6KRg-uMY-s68CUZIG"
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
        print("??")
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


NUM_USERS = 9001
INCREMENT = 1801

# code used to retrieve random ids
# import random
# random.seed("BOOBIES")

# ids = set()

# def createUsers():
#     print("starting query")
#     response = makeQuery("""
#     SELECT DISTINCT OwnerUserId FROM Posts
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
import math

FILE_NAME = "data/UserIds.json"

# write all userids
# with open(FILE_NAME, 'w') as f:
#     json.dump(ids, f)

# load all usersids
with open(FILE_NAME, 'r') as f:
    ids = json.load(f)

content = None

# with open('Posts.csv', 'w', newline='') as csvfile:
#     # Create a CSV writer object
#     writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)  

    # for i in range(1, int(math.ceil(NUM_USERS/INCREMENT) + 1)):
    #     low = INCREMENT * (i - 1)
    #     high = INCREMENT * i

    #     userIds = ids[low:high]
#         query = f""" 
#         SELECT * FROM Posts WHERE OwnerUserId IN ({', '.join(map(str, userIds))});
#         """

#         response = makeQuery(query)

#         print("done")
#         if content == None:
#             columns = []
#             for column in response.get("columns"):
#                 columns.append(column.get("name"))

#             # Write the header row
#             writer.writerow(columns)

#         # Loop through the JSON data and write each row to the CSV file
#         for row in response.get("rows"):
#             writer.writerow(row)

#         print(len(response.get("rows")))


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

#     for i in range(1, int(math.ceil(NUM_USERS/INCREMENT) + 1)):
#         low = INCREMENT * (i - 1)
#         high = INCREMENT * i

#         userIds = ids[low:high]
#         query = f""" 
#         SELECT * FROM Badges WHERE UserId IN ({', '.join(map(str, userIds))});
#         """

#         response = makeQuery(query)

#         columns = []
#         for column in response.get("columns"):
#             columns.append(column.get("name"))

#         # Write the header row
#         writer.writerow(columns)

#         # Loop through the JSON data and write each row to the CSV file
#         for row in response.get("rows"):
#             writer.writerow(row)

#         print(len(response.get("rows")))


with open('data/Comments.csv', 'w', newline='') as csvfile:
    # Create a CSV writer object
    writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)

    for i in range(1, int(math.ceil(NUM_USERS/INCREMENT) + 1)):
        low = INCREMENT * (i - 1)
        high = INCREMENT * i

        userIds = ids[low:high]
        query = f""" 
        SELECT * FROM Comments WHERE UserId IN ({', '.join(map(str, userIds))});
        """

        response = makeQuery(query)

        columns = []
        for column in response.get("columns"):
            columns.append(column.get("name"))

        # Write the header row
        writer.writerow(columns)

        # Loop through the JSON data and write each row to the CSV file
        for row in response.get("rows"):
            writer.writerow(row)

        print(len(response.get("rows")))
