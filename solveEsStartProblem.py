import sys
import os

def validate(path):
    os.chdir(path)

    if  os.path.exists('node.lock'):
        os.system('rm -rf node.lock')


directorys=['/data'+str(i+1)+'/esdata/nodes/0' for i in range(12)]
for directory in directorys:
    validate(directory)


