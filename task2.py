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


bucketname = 'geolacket' #nombre del bucket en el IBM cloud, 'geolacket'or 'damianmaleno'
N_SLAVES = 5 #nunca mas de 100

def master(x , ibm_cos):
    write_permision_list = []
    requests_list=[]
    requests=True

    #Creamos el archivo result.json
    try:
        ibm_cos.get_object(Bucket=bucketname, Key="result.json")
    except:
        ibm_cos.put_object(Bucket=bucketname, Key="result.json", Body=json.dumps(""))

    # 1. monitor COS bucket each X seconds
    while requests:                          #he probado hacerlo con un metacaracter pero no deja
        time.sleep(x)
        try:
            contentsDict=ibm_cos.list_objects_v2(Bucket=bucketname, Prefix='p_write')['Contents'] #La funcion list_object devuelve un diccionario si encuentro el prefijo, sino salta una excepcion
               
    # 2. List all "p_write_{id}" files
            nameFormat=re.compile("p_write_\d") #Comprobaremos que el fichero realmente tiene el formato adecuado
            for file in contentsDict:
                if (nameFormat.match(file['Key'])):
                    requests_list.append(dict(list(file.items())[:2])) #Solo guardamos los campos de "Key" y "LastModified"

    # 3. Order objects of the list "p_write"
            requests_list=sorted(requests_list, key=lambda item: item['LastModified']) #Lo ordena del mas antiguo al mas reciente
    # 4. Pop first object of the list "p_write_{id}"
            next_slave=requests_list.pop(0) #Pop el primero en la lista, es decir, el mas antiguo
    # 5. Write empty "write_{id}" object into COS
            slave_name=next_slave['Key'] #p_write_4
            slave_id = int("".join(list(filter(str.isdigit,slave_name)))) #obtener id del p_write
            permission=f"write_{slave_id}"
            ibm_cos.put_object(Bucket=bucketname, Key=permission)
    # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
            ibm_cos.delete_object(Bucket=bucketname, Key=slave_name)
            write_permision_list.append(slave_id)
    # 7. Monitor "result.json" object each X seconds until it is updated
            found=False
            while not found:
                result = json.loads(ibm_cos.get_object(Bucket=bucketname, Key="result.json")['Body'].read().decode('utf-8'))
                lastID = result.split(" ")[-1]
                if (lastID==slave_id):
                    found=True
                time.sleep(x)
    # 8. Delete from COS “write_{id}”
            ibm_cos.delete_object(Bucket=bucketname, Key=permission)
    # 8. Back to step 1 until no "p_write_{id}" objects in the bucket
            if not requests_list:
                requests=False
        except:
            print("No requests yet")

    return write_permision_list
    #return requests_list
    #return write_permision_list


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
        result = result+f" {id}"
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
    #p=re.compile("p_write_\d")
    #contentsDict=ibm_cos.list_objects_v2(Bucket=bucketname, Prefix='p_write')['Contents']
    #name=re.compile("p_write_\d")
    #ok=False
    #for file in contentsDict:
    #    if (name.match(file['Key'])):
    #        ok=True
    #        break
    #result = ibm_cos.get_object(Bucket=bucketname, Key=key)
    #return contentsDict
    #return json.loads(ibm_cos.get_object(Bucket=bucketname, Key="obj2")['Body'].read())

    
    

if __name__ == '__main__':

    pw = pywren.ibm_cf_executor()
    #pw.call_async(my_function, [bucketname, 'p_write_8'])
    #print(pw.get_result())

    pw.call_async(master, 1)
    pw.map(slave, range(N_SLAVES), )
    write_permission_list = pw.get_result()
    pw.clean()


    # Get result.txt
    # check if content of result.txt == write_permission_list