import random 
import string

# helper function
def generate_random_string(length):
    random_string = ''.join(random.choices(string.ascii_letters + string.digits, k=length))
    return random_string

numOfPeopleEntries = 200 

# generate data for People --- set of all people, and Connected --- subset of People with those who have app
with open("People.txt", "w") as p, open("Connected.txt", "w") as c, open("People_with_handshake_info.txt", "w") as info:
    for i in range (1, numOfPeopleEntries + 1):
        #schema is: id,x,y,name,age,email
        id = i

        x = random.randint(1, 15000)
        y = random.randint(1, 15000)

        name = generate_random_string(6)
        age = random.randint(18, 80)
        email = generate_random_string(8) + "@gmail.com"

        str = f'{id},{x},{y},{name},{age},{email}\n'

        p.write(str)

        #decide if person is connected, if so we will add to Connected too
        connected = random.randint(0, 9)
        if(connected == 1):
            c.write(str)
            connStr = "yes"
        else:
            connStr = "no"

        #add to People_with_handshake_info -- using design option 1
        #schema is: id,x,y,name,age,email,handshake
        info.write(f'{id},{x},{y},{name},{age},{email},{connStr}\n')

