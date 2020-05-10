from cassandra.cluster import Cluster
import cassandra.query as cq
from uuid import UUID,uuid4
import sys, os, gzip, re


def convert(time):
    months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec']
    (date,hour,min,sec)=tuple(time.split(":"))
    spt = date.split("/")
    month = months.index(spt[1])+1
    datetime = spt[2]+"-"+str(month)+"-"+spt[0]+" "+hour+":"+min+":"+sec
    return datetime


def main (input_dir,keyspace,table):
    for f in os.listdir(input_dir):
        with gzip.open(os.path.join(input_dir, f), 'rt', encoding='utf-8') as logfile:
            batch = cq.BatchStatement()
            for line in logfile:
                line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
                w = re.split(line_re,line)
                if len(w) == 6:
                    batch.add(cq.SimpleStatement("INSERT INTO nasalogs (id,host,bytes,datetime,path) VALUES (%s, %s,%s, %s, %s)"), (UUID(int=uuid4().int), w[1], int(w[4]), convert(w[2]), w[3]))
                    if len(batch)%350 == 0:
                        addrows = session.execute(batch)
                        batch.clear()
            addrows = session.execute(batch)
            rows = session.execute('SELECT path, bytes FROM nasalogs WHERE host=%s', ['in24.inetnebr.com'])


if __name__ == '__main__':
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    table = sys.argv[3]
    cluster = Cluster(['199.60.17.188', '199.60.17.216'])
    session = cluster.connect(keyspace)
    truncate = session.execute("TRUNCATE nasalogs;")
    main(input_dir,keyspace,table)
