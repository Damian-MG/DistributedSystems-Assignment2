import pywren_ibm_cloud as pywren
import numpy as np
from io import StringIO
import time
import json
import re

__author__      = "Geovanny Risco y Damian Maleno"
__credits__     = ["Geovanny Risco", "Damian Maleno"]
__version__     = "1.0"
__email__       = ["geovannyalexan.risco@estudiants.urv.cat", "franciscodamia.maleno@estudiants.urv.cat"]
__status__      = "Developping"


bucketname = 'damianmaleno' #nombre del bucket en el IBM cloud, 'geolacket'or 'damianmaleno'
N_SLAVES = 10

def master(id, x , ibm_cos):
    write_permision_list = []

    # 1. monitor COS bucket each X seconds
    requested = False                           #he probado hacerlo con un metacaracter pero no deja
    while True:
        for i in range (1, N_SLAVES+1)
            try:
                if (ibm_cos.get_object(Bucket=bucketname, Key=f"p_write_{i}")):
                    requested = True
                    break
            except:
                print("Waiting for request.")
        time.sleep(x);    
    # 2. List all "p_write_{id}" files
    if (requested)
        j = 0
        for i in range (1, N_SLAVES+1)
            try:
                if (ibm_cos.get_object(Bucket=bucketname, Key=f"p_write_{i}")):
                    write_permision_list[j] = ibm_cos.get_object(Bucket=bucketname, Key=f"p_write_{i}")
                    j++
            except:
                print("")    

    # 3. Order objects of the list "p_write"
    # 4. Pop first object of the list "p_write_{id}"
    # 5. Write empty "write_{id}" object into COS
    # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
    # 7. Monitor "result.json" object each X seconds until it is updated
    # 8. Delete from COS “write_{id}”
    # 8. Back to step 1 until no "p_write_{id}" objects in the bucket

    return write_permision_list


def slave(id, x, ibm_cos):
    # 1. Write empty "p_write_{id}" object into COS
    ibm_cos.put_object(Bucket=bucketname, Key=f"p_write_{id}")
    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    granted = False
    while True:
        try:
            if(ibm_cos.get_object(Bucket=bucketname, Key=f"write_{id}")):
                granted=True
                break
        except:    
            print("Waiting for permission.")
        
        time.sleep(x)

    # 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
    if (granted):
        result = json.loads(ibm_cos.get_object(Bucket=bucketname, Key="result.json")['Body'].read().decode('utf-8'))
        result = result+" "+id
        ibm_cos.put_object(Bucket=bucketname, Key="result.json", Body=json.dumps(result))
    # 4. Finish
    # No need to return anything

def my_function(bucketname, key, ibm_cos):

    #data=json.dumps("123456789")
    #ibm_cos.put_object(Bucket=bucketname, Key=key, Body=data)
    #result = ibm_cos.get_object(Bucket=bucketname, Key=key)['Body'].read()
    #result = json.loads(result)
    #result=result+" "+"Alex"
    #data= json.dumps(result)
    #ibm_cos.put_object(Bucket=bucketname, Key="obj2.json", Body=data)
    ibm_cos.put_object(Bucket=bucketname, Key=key)
    result = ibm_cos.get_object(Bucket=bucketname, Key='p_write_{*}')
    return result
    #return json.loads(ibm_cos.get_object(Bucket=bucketname, Key="obj2")['Body'].read())

    
    

if __name__ == '__main__':

    pw = pywren.ibm_cf_executor()
    pw.call_async(my_function, [bucketname, 'p_write_{id}'])
    print(pw.get_result())

    # pw.call_async(master, 0)
    # pw.map(slave, range(N_SLAVES))
    # write_permission_list = pw.get_result()

    # Get result.txt
    # check if content of result.txt == write_permission_list