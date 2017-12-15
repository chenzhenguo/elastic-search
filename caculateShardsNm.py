
def getShardNum(totalData,replies=1,bestNmPerShard=30):
   capacity=0
   recordsPerG=10000
   capacity=divmod(totalData,recordsPerG)[0]
   print capacity
   replies+=1
   shardNum=replies*divmod(capacity,bestNmPerShard)[0]

   print divmod(capacity,(replies*bestNmPerShard))
   return shardNum



if  __name__=='__main__':
    print 'how many machine ,how mangy records ,and which index you will use'
    print ''' you can choose from [keti,qlr,ql]  '''
    print 'you need %d shards' % getShardNum(3000000)


