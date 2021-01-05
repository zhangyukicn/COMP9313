## import modules here

########## Question 1 ##########
# do not change the heading of the function


'''
def match_function(data_hashes,query_hashes,alpha_m,offset):
    count = 0
    offset_neg = 0 - offset
    for i in range(0,len(data_hashes)):
        if offset_neg <= data_hashes[i] - query_hashes[i] <= offset:
            count = count +1
    if count >= alpha_m:
        return True
    else:
        return False
'''
''' 
way 1
def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    length = -1
    candidates = None

    while length < beta_n:

        offset = offset + 1
        #candidates = data_hashes.filter(lambda x: match_function(x[1],query_hashes,alpha_m,offset)).map(lambda x: x[0])
        candidates = data_hashes.flatMap(lambda x: [x[0]] if match_function(x[1],query_hashes,alpha_m,offset) else [])
        #length = len(candidatesRDD.collect())
        #count():returns the number of rows in this DataFrame.
        length = candidates.count()

    return candidates
'''

'''
def c2lsh(data_hashes, query_hashes, alpha_m, beta_n):
    offset = -1
    #length = -1
    #candidates = None

    while True:
        candidates = data_hashes.flatMap(lambda x: [x[0]] if match_function(x[1],query_hashes,alpha_m,offset) else [])
        #count():returns the number of rows in this DataFrame.
        length = candidates.count()

        if length >= beta_n:
            break
        else:
            offset = offset + 1


        #offset = offset + 1
        #candidates = data_hashes.filter(lambda x: match_function(x[1],query_hashes,alpha_m,offset)).map(lambda x: x[0])
        #candidates = data_hashes.flatMap(lambda x: [x[0]] if match_function(x[1],query_hashes,alpha_m,offset) else [])
        #length = len(candidatesRDD.collect())
        #count():returns the number of rows in this DataFrame.
        #length = candidates.count()

    return candidates
    

'''
'''
def collision_count(a,b,offset):
    counter = 0
    for i in range(len(a)):
        if abs(a[i]-b[i] <= offset):
            counter +=1
    return counter

def c2lsh(data_hashes,query_hashes,alpha_m,beta_n):
    offset = 0
    cand_num = 0
    while cand_num <beta_n:
        candidates = data_hashes.flatMap(lambda x: x[0] if collision_count(x[1],query_hashes,offset)>=alpha_m else [])
        cand_num = candidates.count()
        offset +=1
    return candidates
'''
