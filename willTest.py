import requests
import time
import numpy as np
import csv

DIRECTORY_NAME = "data/"


AUTH_TOKENS = [
    "03AL8dmw9hdZaGG5gEmtEcg_AdG-aMwkHoTLynRCTQ70pzsBgnRNytmzAkYK1FHxftm2KLTj_DoXxVrALoYC254fN6mRD5a9-bZGC-PDdvyd6AHLbu6IihkePk4FDSAoc6nuC4fWh5Ux74Vv4aK5qv0ICLC5aN3T14Sl6BCCzzt2BBq-3y-KDaQMm86L7_mQPQgPQVK2RqkBm-TwlfuJ8U3mxX0WjUVe4DOs1Ibe58q938ouPJPsZf6ehpK0RGFi2QY3XQDIBC0YA0iwQLo6aQbD6AiWykmT-ZwRHTplCoUk9ywabhhv5nzJ7EgIKlOye1MboaN34bm5U5LM9KieRu4BY0sIXYKueTvk0nDcYUQWHG_ZOq1YBLqAvPm9aBzxVTxD8ROnsuzkGptrcriaf76PaoINcJgkBStWQeSGH9I6sP18ibXqWxj8wHDSAB9E931iRCZbQmALt4gNk5PYWWxFYdjpcI4X9DLzrs3PJGKpponMLZgLogL1SHBOUhdxTNUDQjXZdI2G9MzjgF3RTHGiM5JjeyF4nAzP32x1TSMcutzU3H6CNw-ORnCPpwCC_gRA0i-crvu6DY",
    "03AL8dmw_MprK5KgNLEhs5DWjHe8SLGiQQIOncw91Hj9m5H72Ui_pFx_MdIWXHYmkXoOtANXorHnPTDSO29EqSTYqt2TrMALH7QqgS9YNPYNMGGaiSFiMrwRdDkG2vu4DrNfNauO4S1VXNAOfRxV2JjqIYTLtbGQlPzYFtFdNcadB7Ub6l71o9dBR_A6Gl6PpMV3q7-IEExNfnvK69RCTIZfVi8Ovx8zUk7CXd58S-lxGKTTGFyCYK0afFUKqs6q3WQtteZ91cV9J_4Z4WNe4-9uZQDg2GBL5gep_DKl-TMileI7BlOYEuHbiw7hSW-oMfXzQe6MAeFqrizObi5rALY8VECh467vswH_qam07OTwB_JPSjtp0BWuMOvkHIZ17zdEEJnSnaSjulfZo6SYpjZDBKLN0HfRMnd5iJBirKu0op8FmVyQe38cIIoN7ILnMq9FbnMcOeCNgj5r9Rmfvq8W41Q6p-dsNsdegvxHo5i7XA3faoaS2JIPwy09q26K87FItJN8Ns23kVJzeaq8LpSMCLxeE-xizyzfNP7fngEX7HDZFI7PQKC8usNO-zxuwXuQVDLM05gyc4"
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
    print(response)
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


NUM_USERS = 9001
INCREMENT = 125

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
        SELECT * FROM Posts WHERE OwnerUserId IN ({', '.join(map(str, userIds))});
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
