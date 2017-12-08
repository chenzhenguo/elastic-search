


recordsPerGMap=dict(oneQlrRecords=808482,oneQlRecords=808480,oneKetiRecords=1342729)

def getShardNum(machineNm,totalData,replies=1,bestNmPerShard=30,indexType='qlr'):
   capacity=0

   if 'qlr' in indexType:
       recordsPerG=recordsPerGMap['oneQlrRecords']
   elif 'ql' in indexType:
       recordsPerG=recordsPerGMap['oneQlRecords']
   else:
       recordsPerG=recordsPerGMap['oneKetiRecords']

   capacity=divmod(totalData,recordsPerG)[0]
   replies+=1
   shardNum=divmod(capacity,(replies*bestNmPerShard))[0]
   return shardNum



if  __name__=='__main__':
    print '10 y need %d shards' % getShardNum(3,1000000000,indexType='keti')


