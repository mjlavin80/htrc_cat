import redis
import gzip

r = redis.StrictRedis(host='localhost', port=6379, db=0)
PIPE = r.pipeline()

def process(line, num):
    processed = False
    row = line.split("\t")
    #https://www.hathitrust.org/hathifiles_description
    v = "volid: %s" % str(num)
    PIPE.set(v, row[0])
    headers = ["volid", "access", "rights", "record_number", "enumeration", "source", \
"source_instition_ri", "oclcs", "isbns", "issns", "lccns", "title", "imprint", \
"rights_reason_code", "date_last_update", "govt_doc", "pub_date", "pub_place", \
"language", "bib_format"]
    try: 
        for h, i in enumerate(row):
            if h > 0:
                PIPE.hset(row[0], headers[h], i)
        processed = True
    except:
        pass
    return (processed)

files = []
#file id corresponds to latest gz file here: https://www.hathitrust.org/hathifiles
with gzip.open('latest.gz', 'rb') as myfile:
    num = 0 
    for line in myfile:
        e = line.decode('UTF-8')
        row = process(e, num)
        num+=1
        if num % 25000 == 0:
            result = PIPE.execute()
        if num == 101:
            break
    try:
        result = PIPE.execute()
    except:
        pass
print(num)
	
