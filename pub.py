import uuid
import random
import string
import os
import time 
from google.cloud import pubsub_v1


os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'sa_credentials.json'

if __name__ == "__main__":
    characters = string.digits + string.ascii_uppercase
    f = open("input.txt", "w")
    f.write("Student_name, Roll_number(10 digit number), registration_number (UUID in caps without hyphens), Branch, Address1, Address2\n")

    for i in range(10):
        n = random.randint(5,15)
        name = ''.join(random.choice(string.ascii_letters) for i in range(n))
        roll_no = ''.join(random.choice(string.digits) for i in range(10))
        reg_no = uuid.uuid4().hex.upper()
        branch = ''.join(random.choice(string.ascii_letters) for i in range(n))
        address1 = ''.join(random.choice(characters) for i in range(n))
        address2 = ''.join(random.choice(string.ascii_letters) for i in range(n))

        data = name+','+roll_no+','+reg_no+','+branch+','+address1+','+address2

        #print(data)
        f.write(data)
        f.write('\n')

    f.close()
    
    # Replace  with your pubsub topic
    pubsub_topic = 'projects/trainingproject-317506/topics/gcp-training-topic'

    # Replace  with your input file path
    input_file = 'input.txt'

    # create publisher
    publisher = pubsub_v1.PublisherClient()

    with open(input_file, 'rb') as ifp:
        # skip header
        header = ifp.readline()  
        
        # loop over each record
        for line in ifp:
            event_data = line   # entire line of input CSV is the message
            print('Publishing {0} to {1}'.format(event_data, pubsub_topic))
            publisher.publish(pubsub_topic, event_data)
            time.sleep(1)    


