import pywren_ibm_cloud as pywren
import numpy as np
from io import StringIO
import time
import json
import ibm_boto3

__author__      = "Geovanny Risco y Damian Maleno"
__credits__     = ["Geovanny Risco", "Damian Maleno"]
__version__     = "1.0"
__email__       = ["geovannyalexan.risco@estudiants.urv.cat", "franciscodamia.maleno@estudiants.urv.cat"]
__status__      = "Developping"


bucketname = 'geolacket' #nombre del bucket en el IBM cloud, 'geolacket'or 'damianmaleno'
N_SLAVES = 5 #nunca mas de 100

def master(x , ibm_cos):
    write_permision_list = []
    requests=True
        
    # 1. monitor COS bucket each X seconds
    while requests:                    
        time.sleep(x)
        requests_list=[]
        try:
            contentsDict=ibm_cos.list_objects_v2(Bucket=bucketname, Prefix='p_write')['Contents'] #La funcion list_object devuelve un diccionario si encuentro el prefijo, sino salta una excepcion
    # 2. List all "p_write_{id}" files
            for file in contentsDict:
                requests_list.append(dict(list(file.items())[:2])) #Solo guardamos los campos de "Key" y "LastModified"
    # 3. Order objects of the list "p_write"
            requests_list=sorted(requests_list, key=lambda item: item['LastModified']) #Lo ordena del mas antiguo al mas reciente
    # 4. Pop first object of the list "p_write_{id}"
            next_slave=requests_list.pop(0) #Pop el primero en la lista, es decir, el mas antiguo
    # 5. Write empty "write_{id}" object into COS
            slave_name=next_slave['Key'] 
            slave_id = int("".join(list(filter(str.isdigit,slave_name)))) #obtener id del p_write
            last_update=ibm_cos.head_object(Bucket=bucketname, Key="result.json")['LastModified'] #Obtenemos la última fecha de modificación del result.json
            ibm_cos.put_object(Bucket=bucketname, Key=f"write_{slave_id}") 
    # 6. Delete from COS "p_write_{id}", save {id} in write_permission_list
            ibm_cos.delete_object(Bucket=bucketname, Key=slave_name)
            write_permision_list.append(slave_id)
    # 7. Monitor "result.json" object each X seconds until it is updated
            while True:
                time.sleep(x)
                try:
                    result = json.loads(ibm_cos.get_object(Bucket=bucketname, Key="result.json",IfModifiedSince=last_update)['Body'].read().decode('utf-8'))
                    lastID = int(result.split(" ")[-1])
                    if (lastID==slave_id):
                        break
                except:
                    print("Waiting for update")
                    
    # 8. Delete from COS “write_{id}”
            ibm_cos.delete_object(Bucket=bucketname, Key=f"write_{slave_id}")
    # 9. Back to step 1 until no "p_write_{id}" objects in the bucket
        except:
            requests=False

    return write_permision_list


def slave(id, x, ibm_cos):
    # 1. Write empty "p_write_{id}" object into COS
    ibm_cos.put_object(Bucket=bucketname, Key=f"p_write_{id}")
    # 2. Monitor COS bucket each X seconds until it finds a file called "write_{id}"
    while True:
        time.sleep(x)
        try:
            if(ibm_cos.get_object(Bucket=bucketname, Key=f"write_{id}")):
                break
        except:    
            print("Waiting for permission.")  
        
    # 3. If write_{id} is in COS: get result.txt, append {id}, and put back to COS result.txt
    result = json.loads(ibm_cos.get_object(Bucket=bucketname, Key="result.json")['Body'].read().decode('utf-8'))
    if result=="": #Comprobación para evitar que añada un espacio inicial
        result = str(id)
    else:
        result = result+" "+str(id)
    ibm_cos.put_object(Bucket=bucketname, Key="result.json", Body=json.dumps(result))
    # 4. Finish
    # No need to return anything

    
if __name__ == '__main__':

    pw = pywren.ibm_cf_executor()
    ibm_cos = pw.internal_storage.get_client()
    
    #Creamos el archivo result.json si no esta creado
    try:
        ibm_cos.get_object(Bucket=bucketname, Key="result.json")
    except:
        ibm_cos.put_object(Bucket=bucketname, Key="result.json", Body=json.dumps(""))
    
    #Start job
    pw.call_async(master, 1)
    pw.map(slave, range(N_SLAVES))
    write_permission_list = pw.get_result()[0]
    print(write_permission_list)
    pw.clean()

    # Get result.txt
    result_json = ibm_cos.get_object(Bucket=bucketname, Key='result.json')['Body'].read().decode('utf-8')
    print(result_json)

    # check if content of result.txt == write_permission_list
    write_permission_list=json.dumps(" ".join(map(str,write_permission_list))) #Hay que pasar la lista a string porque result.json tiene formato string
    if (result_json==write_permission_list):
        print("Good job!")