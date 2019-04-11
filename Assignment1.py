# Import mpi so we can run on more than one node and processor
from mpi4py import MPI
# Import regular expressions to look for topics and mentions, json to parse tweet data
import re, json, operator


MASTER_RANK = 0


def getMelbGrid(input_file):
  melbGrid={}
  with open(input_file, 'r') as f:
    data = json.load(f)
    features=data['features']
    for i, line in enumerate(features):
      coordinatesList=[]
      id=line['properties']['id']
      coordinatesList.append(line['properties']['xmin'])
      coordinatesList.append(line['properties']['xmax'])
      coordinatesList.append(line['properties']['ymin'])
      coordinatesList.append(line['properties']['ymax'])
      melbGrid[id]=coordinatesList
  return melbGrid

# def distributeCoordinate(tweet,melbGrid):
#   result={'A1':0,'A2':0,'A3':0,'A4':0,'B1':0,'B2':0,'B3':0,'B4':0,'C1':0,'C2':0,'C3':0,'C4':0,'C5':0,'D3':0,'D4':0,'D5':0}
#   coordinates=tweet[0]
#   hashtags=tweet[1]
#   for coordinatesID,coordinates in coordinates.items():
#     for cells,ranges in melbGrid.items():
#       if (coordinates[0]>ranges[0] and coordinates[0]<=ranges[1]) and(coordinates[1]>ranges[2] and coordinates[1]<=ranges[3]):
#         result[cells]+=1
#   return result
      

# def countTweets(cellsAndHashtags):
#   result={}
#   for cells in cellsAndHashtags:
#     if cells[0] in result.keys():
#       result[cells[0]]+=1
#     else:
#       result[cells[0]]=1
#   return result
#result里面是{'A1':['Melbourne','fb'],'A2':['Melbourne','fb']}这种格式的
#本函数的返回值为{'A1':[('Melbourne',100),('FB',99),...],'A2':[('Melbourne',100)]}里面的数据为元组类型
# def countHashtags(cellsAndHashtags):
#   result={}
#   finalResult={}
#   for cells in cellsAndHashtags:
#     if cells[0] in result.keys():
#       result[cells[0]].append(cells[1])
#     else:
#       result[cells[0]]=[cells[1]]

#   for cell,hashtags in result.items():
#     count={}
#     for hashtag in hashtags:
#       if hashtag in count.keys():
#         count[hashtag]+=1
#       else:
#         count[hashtag]=0
#     sortedCount=sorted(count.items(),key = lambda x:x[1],reverse = True)[:5]
#     finalResult[cell]=sortedCount

#   return finalResult

def main():
  # Get
  input_file = 'data/bigTwitter.json'
  # Work out our rank, and run either master or slave process
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()
  if rank == 0 :
    # We are master
    master_tweet_processor(comm, input_file)
  else:
    # We are slave
    slave_tweet_processor(comm, input_file)


# def process_tweets(rank, input_file, processes):
#   with open(input_file, 'r') as f:
#     data = json.load(f)
#     rows=data['rows']
#     coordinates = {}
#     hashtags={}
#     # Send tweets to slave processes
#     for i, line in enumerate(rows):
#       if i%processes == rank:
#         hashtag=[]
#         id=line['id']
#         coordinates[id] = line['value']['geometry']['coordinates']
#         for x in line['doc']['entities']['hashtags']:
#           hashtag.append(x['text'])
#         hashtags[id]=hashtag
#   return [coordinates,hashtags]
def process_tweets(rank, input_file, processes,melbGrid):
  result={'A1':0,'A2':0,'A3':0,'A4':0,'B1':0,'B2':0,'B3':0,'B4':0,'C1':0,'C2':0,'C3':0,'C4':0,'C5':0,'D3':0,'D4':0,'D5':0}
  hashtags={'A1':{},'A2':{},'A3':{},'A4':{},'B1':{},'B2':{},'B3':{},'B4':{},'C1':{},'C2':{},'C3':{},'C4':{},'C5':{},'D3':{},'D4':{},'D5':{}}
  f=open(input_file, 'r',encoding='utf-8')
  i=0
  for lines in f:
    if i%processes == rank:
      line=''
      if i!=0 and len(lines)>10:
        if lines[-2]==",":
          line=lines[:-2]
        else:
          line=lines
      else:
        i+=1
        continue
      line=json.loads(line)
      countHashtages={}
      # id=line['id']
      doc=line['doc']
      if isinstance(doc,dict):
        coordinates=line['doc']['coordinates']
        if isinstance(coordinates,dict):
          singalCoordinate=line['doc']['coordinates']['coordinates']
          if isinstance(singalCoordinate,list):
            cell='A0'
            if singalCoordinate[1]==-37.5:
              if singalCoordinate[0]>=144.7 and singalCoordinate[0]<=144.85:
                cell='A1'
              if singalCoordinate[0]>144.85 and singalCoordinate[0]<=145:
                cell='A2'
              if singalCoordinate[0]>145 and singalCoordinate[0]<=145.15:
                cell='A3'
              if singalCoordinate[0]>145.15 and singalCoordinate[0]<=145.3:
                cell='A4'
            if singalCoordinate[0]==144.7:
              if singalCoordinate[1]>=-37.95 and singalCoordinate[1]<-37.8:
                cell='C1'
              if singalCoordinate[1]>=-37.8 and singalCoordinate[1]<-37.65:
                cell='B1'
              if singalCoordinate[1]>=-37.65 and singalCoordinate[1]<=-37.5:
                cell='A1'
            if singalCoordinate[0]==145:
              if singalCoordinate[1]>=-38.1 and singalCoordinate[1]<-37.95:
                cell='D3'
            if singalCoordinate[1]==-37.8:
              if singalCoordinate[0]>145.3 and singalCoordinate[0]<=145.45:
                cell='C5'
            if cell=='A0':
              for cells,ranges in melbGrid.items():
                if ((singalCoordinate[0]>ranges[0]) and (singalCoordinate[0]<=ranges[1])) and ((singalCoordinate[1]>=ranges[2]) and (singalCoordinate[1]<ranges[3])):
                  hashtag=line['doc']['entities']['hashtags']
                  if hashtag!=[]:
                    for x in hashtag:
                      if x['text'] in countHashtages.keys():
                        countHashtages[x['text']]+=1
                      else:
                        countHashtages[x['text']]=1
                  for k,v in countHashtages.items():
                    if k in hashtags[cells]:
                      hashtags[cells][k]+=1
                    else:
                      hashtags[cells][k]=1
                  result[cells]+=1
            else:
              result[cell]+=1
              hashtag=line['doc']['entities']['hashtags']
              if hashtag!=[]:
                for x in hashtag:
                  if x['text'] in countHashtages.keys():
                    countHashtages[x['text']]+=1
                  else:
                    countHashtages[x['text']]=1
              for k,v in countHashtages.items():
                if k in hashtags[cell]:
                  hashtags[cell][k]+=1
                else:
                  hashtags[cell][k]=1
        # for x in line['doc']['entities']['hashtags']:
        #   hashtag.append(x['text'])
        # hashtags[id]=hashtag
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

def master_tweet_processor(comm, input_file):
    # Read our tweets
    rank = comm.Get_rank()
    size = comm.Get_size()
    melbGrid=getMelbGrid('melbGrid.json')
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
    print(coordinates)
    print('-------------------')
    print(hashtags)

def slave_tweet_processor(comm,input_file):
  # We want to process all relevant tweets and send our counts back
  # to master when asked
  # Find my tweets
  rank = comm.Get_rank()
  size = comm.Get_size()

  melbGrid=getMelbGrid('melbGrid.json')
  result=process_tweets(rank, input_file,size,melbGrid)

  # Now that we have our counts then wait to see when we return them.
  while True:
    in_comm = comm.recv(source=MASTER_RANK, tag=rank)
    # Check if command
    if isinstance(in_comm, str):
      if in_comm in ("return_data"):
        # Send data back
        # print("Process: ", rank, " sending back ", len(counts), " items")
        comm.send(result, dest=MASTER_RANK, tag=MASTER_RANK)
      elif in_comm in ("exit"):
        exit(0)
# Run the actual program
if __name__ == "__main__":
  main()