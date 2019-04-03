import json
import numpy as np

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
print(read_grids('melbGrid.json'))
