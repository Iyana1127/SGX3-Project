import requests


response = requests.get("http://35.206.76.195:8067/head?count=10")


#Check to see if it works!
print(response.status_code)
print(response.headers)

#view data
data = response.json()
print (type(data))
print (data)
