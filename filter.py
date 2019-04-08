import json

from mpi4py import MPI
import time

comm = MPI.COMM_WORLD
size = comm.Get_size()
rank = comm.Get_rank()
start_time = time.time()

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
    grid_data["count"] = 0
    # Not too sure if coordinates needed here. For now I just ignored it. 
    # grid_data["coordinates"] = lines["geometry"]["coordinates"]

    grid.append(grid_data)
  return grid
# Simple print test for output. Seems correct. 

grid = read_grids('melbGrid.json')

# Count file_length. The result is how many tweets there are. 
# Big twiter has 250 0000 linee. 
def count_file_length():
    with open('bigTwitter_sample.json', 'r', encoding='utf-8') as f:
      for line in f:
        length = -2
        while line:
          length+=1
          line = f.readline()
        f.close()
        return length
print(count_file_length())

# 经度纬度： longtitude and latitude. 
# longtitude 是 -180 到 180， latitude 是 -90 到 90.
# 如果要拿到 hash tag:
# ["doc"]["text"] -> 得到raw text
# 如果要拿到 coordinates:
# 首先要检测是不是valid， 但是 bigTwitter里面有两个地方有他的坐标， 
# 1. ["doc"]["coordinates"]["coordinates"] --- 之后的再取0 或1. [经度， 纬度]
# 2. ["doc"]["geo"]["coordinates"]  --- [纬度，经度]

# 我个人感觉最好是0存纬度， 1存经度。 


# For now, for simplicity reason, I will split the file into chunks naively, assuming that there is no mistakes in the file such that the line /n is correct. But I do believe that we have to handle the exception or at least try to handle the expception when new line character is not proper?
# 或者说，要保证每一行的断行是完整的。 读到
# chunks = file_length/ sise(number of processors),which is the size that a core should do. 
def split_and_read(length, chunks):

  coordinates = []
  result = {}
  # Each core should get the different start and end variable to process. 
  if rank != size -1:
    start = int(rank * chunks)
    end = int(rank * chunks + chunks)
  # If it is the last rank. 
  else:
    start = int(rank * chunks)
    end = length
  with open('bigTwitter_sample.json', encoding='utf-8') as file:
    # print("This is No.%d Rank and this is the start:%d"%(rank,start))
    # print("This is No.%d Rank and this is the end: %d"%(rank,end))

    # 之前忽略了这个问题。 若如果不把这一行读掉，最后的json会少一行。 
    file.readline()# 空过第一行. 可是会不会多个task直接也每一个都少读了一行呢？


    # 这里有个bug，第一行我感觉要空过去，可是加不加一呢？？
    for e in range(0,start): 
      file.readline()
      # print("FIRST Loop, RANK: %d START: %d END: %d  %s"%(rank,start,end,file.readline()))
      # file.readline() # remember to skip the first line.
      # print(file.readline())
    for e in range(start, end):
      # print("SECOND Loop, RANK: %d START: %d END: %d %s"%(rank,start,end,file.readline()))
      # print(file.readline())
      json_content = file.readline()
      try:
        one_coord = {}
        one_json = json.loads(json_content[:-2])
        # Deal with the case when the coordinates dont' exist. 
        try:
          one_coord["latitude"] = one_json["doc"]["coordinates"]["coordinates"][1]
          one_coord["longtitude"] = one_json["doc"]["coordinates"]["coordinates"][0]
          if one_coord["latitude"] == None or one_coord["longtitude"] == None:
            one_coord["latitude"] = one_json["doc"]["geo"]["coordinates"][0]
            one_coord["longtitude"] = one_json["doc"]["geo"]["coordinates"][1]
        except:
          pass
        # Filter out the coordinates that are not in the grid. 
        if(if_coor_in_grid(one_coord["latitude"],one_coord["longtitude"])):
          coordinates.append(one_coord)
      # Special case for the last json.
      except:
        one_coord = {}
        one_json = json.loads(json_content[:-1])
        try:
          one_coord["latitude"] = one_json["doc"]["coordinates"]["coordinates"][1]
          one_coord["longtitude"] = one_json["doc"]["coordinates"]["coordinates"][0]
          # Test if one of the coordinate is not properly stored/corrupted, Try another way to get the coordinates. 
          if one_coord["latitude"] == None or one_coord["longtitude"] == None:
            one_coord["latitude"] = one_json["doc"]["geo"]["coordinates"][0]
            one_coord["longtitude"] = one_json["doc"]["geo"]["coordinates"][1]
        except:
          pass
        # Filter out the coordinates that are not in the grid. 
        if(if_coor_in_grid(one_coord["latitude"],one_coord["longtitude"])):
          coordinates.append(one_coord)
  # for p in coordinates:
  #   for 

  # print("This is the coordinate list: ",coordinates)
  for g in grid:
    print("ID: %s  ,Count: %s  "%(g["id"],g["count"]))
  final_result = comm.gather(grid,root=0)
  file.close()

# Helper function for filtering out the data that is not in the grid. 
# Need to handle the boundry case. 
def if_coor_in_grid(lat,long):
  for block in grid:
    if (lat >= block["ymin"]) and (lat <= block["ymax"]) and (long >= block["xmin"]) and (long <= block["xmax"]):
      # print(block)
      block["count"] +=1
      return True







      

  

file_length = count_file_length()
chunks = file_length/size
split_and_read(file_length, chunks)


print(str(time.time()-start_time))



# print(grid)
# for g in grid:
#   print(g)

# partition = split_size(file_length, chunks)
# print(partition[-1])

# Main function, performing " #tag " extraction and coordinates extraction. 
# def extract_information():
#   with open('smallTwitter.json', encoding='utf-8') as file:
#     for e in range(0, e)



# # print(grid)

# # test_json = 'tinyTwitter.json'
# # f = open(test_json, 'r')

# def divide_location_to_grids(grid, latitude, longtitude):
#   for data in grid:
#     # 貌似没有考虑到边界值的问题？
#     # 这里应该怎么维护边界原则？
#     # 可以单独列出两种可能性， 一种是在最上面一种是最左边， 但是如何提取到上面和左边的grid 的id呢？ 
#     # 可不可以通过 坐标的相加减？？？？？----- 可行。 用elif再写两种情况，然后要挺麻烦的计算。 
#     if(latitude> data["ymin"] and latitude < data["ymax"] and longtitude > data["xmin"] and longtitude < data["xmax"]):
#       data["count"] = data["count"] + 1



# # print(divide_location_to_grids(grid, ))





# # For now it is tinyTwitter.json. 
# file = 'tinyTwitter.json'



# # This is the json format for extracting tweet content. 
# # print(x["value"]["properties"]["text"])


# # The us of Try--Except can skip those don't have coordinates. 
# # Later this can also skip those don't have Hash Tags. 
# def read_tweet_json(file):
#   # First case when 1 node and 1 core
#   coords = []
#   if size <=1 and rank == 0:
#     with open(file, encoding='utf-8') as f:
#       for row in f:
#         try:
#           one_record = {}
#           one_json = json.loads(row[:-2])
#           one_record["latitude"] = one_json["value"]["geometry"]["coordinates"][0]
#           one_record["longtitude"] = one_json["value"]["geometry"]["coordinates"][1]
#           coords.append(one_record)
#         except:
#           try: # Need to handle the last line. 
#             one_record = {}
#             one_json = json.loads(row[:-1])
#             one_record["latitude"] = one_json["value"]["geometry"]["coordinates"][0]
#             one_record["longtitude"] = one_json["value"]["geometry"]["coordinates"][1]
#             coords.append(one_record)
#           except:
#             continue
#   elif rank == 0: #Parallel. Need to split the loaded json into chunks(depends on how many cores you have. )
#     with open(file, encoding = 'utf-8') as f_:
#       for row in f_:
#         try:
#           one_record = {}
#           one_json = json.loads(row[:-2])
#           one_record["latitude"] = one_json["value"]["geometry"]["coordinates"][0]
#           one_record["longtitude"] = one_json["value"]["geometry"]["coordinates"][1]
#           coords.append(one_record)
#         except:
#           try:
#             one_record = {}
#             one_json = json.loads(row[:-1])
#             one_record["latitude"] = one_json["value"]["geometry"]["coordinates"][0]
#             one_record["longtitude"] = one_json["value"]["geometry"]["coordinates"][1]
#             coords.append(one_record)
#           except:
#             continue
#     coords = np.array_split(coords, size)
#   else:
#     coords = None
#   return coords

# coords = read_tweet_json(file)

# # def split_reading():
  
# # print(read_tweet_json(file))

# # Master:
# if size < 2 and rank ==0:
#   None
#   # for e in coords:
#   #   divide_location_to_grids(grid, e["latitude"], e["longtitude"])

# else:  # slave node. Need to scatter task. 
#   coords = comm.scatter(coords, root=0)
#   for e in coords:
#     divide_location_to_grids(grid, e["latitude"], e["longtitude"])
#     new_coords = comm.gather(coords, root=0)

# # Used for testing purpose. 
# print(new_coords)

# f3 = open('output.txt', 'w')
# f3.write(str(time.time() - start_time))
# f3.write(str(new_coords))
# f3.close()