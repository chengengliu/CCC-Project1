import json
import numpy as np
from mpi4py import MPI

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()

def read_grids(name):
  grid = []
  file = open(name, 'r')
  json_data = json.load(file)
  for lines in json_data["features"]:
    grid_data = {}
    grid_data["id"] = lines["properties"]["id"]
    grid_data["xmin"] = lines["properties"]["xmin"]
    grid_data["xmax"] = lines["properties"]["xmax"]
    grid_data["ymin"] = lines["properties"]["ymin"]
    grid_data["ymax"] = lines["properties"]["ymax"]

    # Not too sure if coordinates needed here. For now I just ignored it. 
    # grid_data["coordinates"] = lines["geometry"]["coordinates"]

    grid.append(grid_data)
  return grid
# Simple print test for output. Seems correct. 

grid = read_grids('melbGrid.json')

print(grid)

# test_json = 'tinyTwitter.json'
# f = open(test_json, 'r')

def divide_location_to_grids(grid, latitude, longtitude):
  for data in grid:
    # 貌似没有考虑到边界值的问题？
    # 这里应该怎么维护边界原则？
    # 可以单独列出两种可能性， 一种是在最上面一种是最左边， 但是如何提取到上面和左边的grid 的id呢？ 
    # 可不可以通过 坐标的相加减？？？？？----- 可行。 用elif再写两种情况，然后要挺麻烦的计算。 
    if(latitude> data["ymin"] and latitude < data["ymax"] and longtitude > data["xmin"] and longtitude < data["xmax"]):
      data["count"] = data["count"] + 1



# print(divide_location_to_grids(grid, ))



# 目前有两种想法：
# 第一种是 一行一行的读入，但是可能会慢。。
# 第二种是 分成几个chunks 来读入。 

# For now it is tinyTwitter.json. 
file = 'tinyTwitter.json'



# This is the json format for extracting tweet content. 
# print(x["value"]["properties"]["text"])


# The us of Try--Except can skip those don't have coordinates. 
# Later this can also skip those don't have Hash Tags. 
def read_tweet_json(file):
  # First case when 1 node and 1 core
  coords = []
  if size <=1 and rank == 0:
    with open(file, encoding='utf-8') as f:
      for row in f:
        try:
          one_record = {}
          one_json = json.loads(row[:-2])
          one_record["latitude"] = one_json["value"]["geometry"]["coordinates"][0]
          one_record["longtitude"] = one_json["value"]["geometry"]["coordinates"][1]
          coords.append(one_record)
        except:
          try: # Need to handle the last line. 
            one_record = {}
            one_json = json.loads(row[:-1])
            one_record["latitude"] = one_json["value"]["geometry"]["coordinates"][0]
            one_record["longtitude"] = one_json["value"]["geometry"]["coordinates"][1]
            coords.append(one_record)
          except:
            continue
  elif rank == 0: #Parallel. Need to split the loaded json into chunks(depends on how many cores you have. )
    with open(file, encoding = 'utf-8') as f_:
      for row in f_:
        try:
          one_record = {}
          one_json = json.loads(row[:-2])
          one_record["latitude"] = one_json["value"]["geometry"]["coordinates"][0]
          one_record["longtitude"] = one_json["value"]["geometry"]["coordinates"][1]
          coords.append(one_record)
        except:
          try:
            one_record = {}
            one_json = json.loads(row[:-1])
            one_record["latitude"] = one_json["value"]["geometry"]["coordinates"][0]
            one_record["longtitude"] = one_json["value"]["geometry"]["coordinates"][1]
            coords.append(one_record)
          except:
            continue
    coords = np.array_split(coords, size)
  else:
    coords = None
  return coords
coords = read_tweet_json(file)
# print(read_tweet_json(file))

# Master:
if size < 2 and rank ==0:
  for e in coords:
    divide_location_to_grids(grid, e["latitude"], e["longtitude"])
    answer = grid
else:  # slave node. Need to scatter task. 
  coords = comm.scatter(coords, root=0)
  for e in coords:
    divide_location_to_grids(grid, e["latitude"], e["longtitude"])
    a = comm.gather(coords, root=0)

print(a)


        
