import json

urls = []
duplicate_list = []
duplicate_count = 0
with open("/Users/sreeja/CS172/CS172-Project/output/reddit_data_0.json", "r") as file:
    lines = file.readlines()
    for line in lines:
        line = json.loads(line)
        url = line["url"]
        if(url in urls):
            duplicate_list.append(url)
            duplicate_count+=1
        else:
            urls.append(url)
        

print(duplicate_count)
print(duplicate_list)
