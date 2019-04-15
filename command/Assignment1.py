# Import mpi so we can run on more than one node and processor
from mpi4py import MPI
# Import regular expressions to look for topics and mentions, json to parse tweet data
import json, operator,time


MASTER_RANK = 0

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
  input_file = 'data/bigTwitter.json'
  # Work out our rank, and run either master or slave process
  comm = MPI.COMM_WORLD
  rank = comm.Get_rank()
  start_time = time.time()
  if rank == 0 :
    # We are master
    master_tweet_processor(comm, input_file)
  else:
    # We are slave
    slave_tweet_processor(comm, input_file)

def process_tweets(rank, input_file, processes,melbGrid):
  result=initialise_grid('result')
  hashtags=initialise_grid('hashtags')

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
                  secondHash = line['doc']['text'].split()
                  # print(secondHash)
                  if secondHash:
                    for word in secondHash:
                      if word[0] == '#':
                        if word in countHashtages.keys():
                          countHashtages[word] +=1
                        else:
                          countHashtages[word] = 1

                  # if hashtag!=[]:
                  #   for x in hashtag:
                  #     if x['text'] in countHashtages.keys():
                  #       countHashtages[x['text']]+=1
                  #     else:
                  #       countHashtages[x['text']]=1
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
              # if hashtag!=[]:
                # for x in hashtag:
                #   if x['text'] in countHashtages.keys():
                #     countHashtages[x['text']]+=1
                #   else:
                #     countHashtages[x['text']]=1
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

    # print(coordinates)
    print("Result:")
    sorted_coordinates = sorted(coordinates.items(), key=lambda kv: kv[1])
    print("Try a sort version of dictionary:???",sorted_coordinates)
    sorted_coordinates.reverse()
    for i in sorted_coordinates:
      print(i[0],":",i[1]," posts.")
      print("Hashtags: ")
      print(hashtags.get(i[0]))
    # for key,item in coordinates.items():
    #   print(key,": ",item)
    print('-------------------')
    # print(hashtags)
    # for key,item in hashtags.items():
    #   print(key,item)

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
        # print("Process: ", rank, " sending back ", len(counts), " items")
        comm.send(result, dest=MASTER_RANK, tag=MASTER_RANK)
      elif in_comm in ("exit"):
        exit(0)
# Run the actual program
if __name__ == "__main__":
  start_time = time.time()
  main()
  print("Tottal time is : ", str(time.time()-start_time))
