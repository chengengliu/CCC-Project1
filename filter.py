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
print(read_grids('melbGrid.json'))

grid = read_grids('melbGrid.json')
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


# Considering the N core case: 
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
          one_record["longtitude"] = one_json["value"]["geometry"]     ["coordinates"][1]
          coords.append(one_record)
        except:
          continue
  return coords

print(read_tweet_json(file))

        
