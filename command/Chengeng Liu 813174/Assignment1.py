# COMP90024 Cluster and Cloud Computing 
# Project 1 
# Author: 
# Chengeng Liu  Student ID : 813174
# Xinze Li      Student ID : 964135

# This program is to process and extract information from an 11 GB json file. 
# The aim is to count the number of twitts within the given range(melbGrid.json)
# and rank the hashtags in these grids. 
# This program uses mpi4py as the core library. 
# The bottleneck of the project is how to deal with io operation as the 
# file is extremely large and it is not feasible to read it into memory as a 
# whole. We adopted a master-slave approach and assign lines to different
# nodes. After all nodes finish their jobs, the master node will collect the 
# result and integrate them. This will utilize all cores and maximize the 
# computability. 

# Core library used in the program. 
from mpi4py import MPI
# json parser and calculate time. 
import json, operator,time


MASTER_RANK = 0
TEN=10
# initialise_grid function used to load melbGrid
# 'Flag' indicates the use of the function, since the function
# maybe used for result initialization as well as 
# initializing hashtag calculation.
def initialise_grid(flag):
  f = open('melbGrid.json','r')
  read_in = json.loads(f.read())
  result= {}
  for g in read_in['features']:
    if flag == 'load':
      result[g['properties']['id']] = [g['properties']['xmin'], g['properties']['xmax'], g['properties']['ymin'], g['properties']['ymax']]
    if flag == 'result':
      result[g['properties']['id']] = 0
    if flag == 'hashtags':
      result[g['properties']['id']] = {}
  f.close()
  return result

def main():
  # Get
  input_file = 'bigTwitter.json'
  # Work out our rank, and run either master or slave process
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()
  start_time = time.time()
  # Master node
  if rank == 0 :
    master_tweet_processor(comm, input_file)
  # Slave node
  else:
    slave_tweet_processor(comm, input_file)

# This is the main function that used to calculate coordinate and hashtages
# The slave node will be assigned to process the line only when it should do 
# so. A counter is used to assign the line reading job. 
def process_tweets(rank, input_file, processes,melbGrid):
  result=initialise_grid('result')
  hashtags=initialise_grid('hashtags')

  f=open(input_file, 'r',encoding='utf-8')
  i=0
  for lines in f:
    if i%processes == rank:
      line=''
      if i!=0 and len(lines)>TEN:
        if lines[-2]==",":
          line=lines[:-2]
        else:
          line=lines
      else:
        i+=1
        continue
      line=json.loads(line)
      countHashtages={}
      doc=line['doc']
      if isinstance(doc,dict):
        coordinates=line['doc']['coordinates']
        if isinstance(coordinates,dict):
          singalCoordinate=line['doc']['coordinates']['coordinates']
          # Consider the case when the data is at the boundry of the grid
          if isinstance(singalCoordinate,list):
            cell='A0'
            if singalCoordinate[1]==melbGrid['A1'][3]:
              if singalCoordinate[0]>=melbGrid['A1'][0] and singalCoordinate[0]<=melbGrid['A1'][1]:
                cell='A1'
              if singalCoordinate[0]>melbGrid['A2'][0] and singalCoordinate[0]<=melbGrid['A2'][1]:
                cell='A2'
              if singalCoordinate[0]>melbGrid['A3'][0] and singalCoordinate[0]<=melbGrid['A3'][1]:
                cell='A3'
              if singalCoordinate[0]>melbGrid['A4'][0] and singalCoordinate[0]<=melbGrid['A4'][1]:
                cell='A4'
            if singalCoordinate[0]==melbGrid['C1'][0]:
              if singalCoordinate[1]>=melbGrid['C1'][2] and singalCoordinate[1]<melbGrid['C1'][3]:
                cell='C1'
              if singalCoordinate[1]>=melbGrid['B1'][2] and singalCoordinate[1]<melbGrid['B1'][3]:
                cell='B1'
              if singalCoordinate[1]>=melbGrid['A1'][2] and singalCoordinate[1]<=melbGrid['A1'][3]:
                cell='A1'
            if singalCoordinate[0]==melbGrid['D3'][0]:
              if singalCoordinate[1]>=melbGrid['D3'][2] and singalCoordinate[1]<melbGrid['D3'][3]:
                cell='D3'
            if singalCoordinate[1]==melbGrid['C5'][3]:
              if singalCoordinate[0]>melbGrid['C5'][0] and singalCoordinate[0]<=melbGrid['C5'][1]:
                cell='C5'
            if cell=='A0':
              for cells,ranges in melbGrid.items():
                if ((singalCoordinate[0]>ranges[0]) and (singalCoordinate[0]<=ranges[1])) and ((singalCoordinate[1]>=ranges[2]) and (singalCoordinate[1]<ranges[3])):
                  # hashtag way of extracting json data is depreciated. 
                  # extracting from raw text is preferred. (secondHash)
                  hashtag=line['doc']['entities']['hashtags']
                  secondHash = line['doc']['text'].split()
                  if secondHash:
                    for word in secondHash:
                      if word[0] == '#':
                        if word in countHashtages.keys():
                          countHashtages[word] +=1
                        else:
                          countHashtages[word] = 1
                  for k,v in countHashtages.items():
                    if k in hashtags[cells]:
                      hashtags[cells][k]+=1
                    else:
                      hashtags[cells][k]=1
                  result[cells]+=1
            else:
              result[cell]+=1
              hashtag=line['doc']['entities']['hashtags']
              secondHash = line['doc']['text'].split()
              if secondHash:
                    for word in secondHash:
                      if word[0] == '#':
                        if word in countHashtages.keys():
                          countHashtages[word] +=1
                        else:
                          countHashtages[word] = 1
              for k,v in countHashtages.items():
                if k in hashtags[cell]:
                  hashtags[cell][k]+=1
                else:
                  hashtags[cell][k]=1
    i=i+1
  return [result,hashtags]

def marshall_tweets(comm):
  processes = comm.Get_size()
  counts = []
  #Now ask all processes except oursevles to return counts
  for i in range(processes-1):
    # Send request
    comm.send('return_data', dest=(i+1), tag=(i+1))
  for i in range(processes-1):
    # Receive data
    counts.append(comm.recv(source=(i+1), tag=MASTER_RANK))
  return counts

# Master node is responsible for collecting and integrating all data from 
# each node and printing out the final result. 
def master_tweet_processor(comm, input_file):
    # Read our tweets
    rank = comm.Get_rank()
    size = comm.Get_size()
    melbGrid= initialise_grid('load')
    result=process_tweets(rank, input_file,size,melbGrid)
    coordinates=result[0]
    hashtags=result[1]
    if size > 1:
      counts = marshall_tweets(comm)
      # Marshall that data
      for oneResult in counts:
        for k,v in oneResult[0].items():
          coordinates[k]+=v
        for location in oneResult[1]:
          temp=hashtags[location].copy()
          for oneHashtag in oneResult[1][location]:
            if oneHashtag in temp:
              temp[oneHashtag]+=oneResult[1][location][oneHashtag]
            else:
              temp[oneHashtag]=oneResult[1][location][oneHashtag]
          hashtags[location]=temp


      # Turn everything off
      for i in range(size-1):
        # Receive data
        comm.send('exit', dest=(i+1), tag=(i+1))
      for a,b in hashtags.items():
        hashtags[a]=sorted(b.items(),key = lambda x:x[1],reverse = True)[:5]
    else:
      for k,v in hashtags.items():
        hashtags[k]=sorted(v.items(),key = lambda x:x[1],reverse = True)[:5]

    # Print output

    # Print out the result based on block.
    print("Result:")
    sorted_coordinates = sorted(coordinates.items(), key=lambda kv: kv[1])
    sorted_coordinates.reverse()
    print(sorted_coordinates)
    for i in sorted_coordinates:
      print(i[0],":",i[1]," posts.")
      print("Hashtags: ")
      print(hashtags.get(i[0]))
    print('-------------------')

# A slave node will always be ready to receive data and tasks
def slave_tweet_processor(comm,input_file):
  # We want to process all relevant tweets and send our counts back
  # to master when asked
  # Find my tweets
  rank = comm.Get_rank()
  size = comm.Get_size()

  melbGrid = initialise_grid ('load')
  result=process_tweets(rank, input_file,size,melbGrid)

  # Now that we have our counts then wait to see when we return them.
  while True:
    in_comm = comm.recv(source=MASTER_RANK, tag=rank)
    # Check if command
    if isinstance(in_comm, str):
      if in_comm in ("return_data"):
        # Send data back
        comm.send(result, dest=MASTER_RANK, tag=MASTER_RANK)
      elif in_comm in ("exit"):
        exit(0)
        
# Run the actual program
if __name__ == "__main__":
  start_time = time.time()
  main()
  print("Tottal time is : ", str(time.time()-start_time))
