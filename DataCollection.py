import requests
import time
import numpy as np
import csv

DIRECTORY_NAME = "data/"


AUTH_TOKENS = [
    "",
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
# random.seed("SLURPY HERPY DERP")

# ids = set()

# def createUsers():
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

import math
import json

FILE_NAME = "data/UserIds.json"

# write all userids
# with open(FILE_NAME, 'w') as f:
#     json.dump(ids, f)

# load all usersids
with open(FILE_NAME, 'r') as f:
    ids = json.load(f)


with open('Posts.csv', 'w', newline='') as csvfile:
    # Create a CSV writer object
    writer = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)  

    for i in range(1, int(math.ceil(NUM_USERS/INCREMENT) + 1)):
        low = INCREMENT * (i - 1)
        high = INCREMENT * i

        userIds = ids[low:high]
        query = f""" 
        SELECT * FROM Posts WHERE OwnerUserId IN ({', '.join(map(str, userIds))});
        """

        response = makeQuery(query)

        print("done")
        if i == 1:
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


