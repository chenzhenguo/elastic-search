


recordsPerGMap=dict(oneGQlrRecords=808482,oneGQlRecords=808480,oneGKetiRecords=1342729)

def getShardNum(machineNm,totalData,replies=1,bestNmPerShard=30,indexType='qlr'):
   capacity=0

   if 'qlr' in indexType:
       recordsPerG=recordsPerGMap['oneGQlrRecords']
   elif 'ql' in indexType:
       recordsPerG=recordsPerGMap['oneGQlRecords']
   else:
       recordsPerG=recordsPerGMap['oneGKetiRecords']

   capacity=divmod(totalData,recordsPerG)[0]
   replies+=1
   shardNum=divmod(capacity,(replies*bestNmPerShard))[0]
   return shardNum



if  __name__=='__main__':
    print '10 y need %d shards' % getShardNum(3,1000000000,indexType='keti')


