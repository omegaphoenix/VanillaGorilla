import random, string;

mode = 'huge'  # 'small', 'large' or 'huge'


# Small:  id = integer, value = 20 chars, 700 rows
# Large:  id = 40 chars, value = 20 chars, 10000 rows
# Huge:   id = 200 chars, value = 20 chars, 20000 rows

if mode == 'small':
    print '''CREATE TABLE test_pk_index (
        id INTEGER PRIMARY KEY,
        value VARCHAR(20)
    );
    '''
    num_rows = 700

elif mode == 'large':
    print '''CREATE TABLE test_pk_index (
        id CHAR(40) PRIMARY KEY,
        value VARCHAR(20)
    );
    '''

    num_rows = 10000

elif mode == 'huge':
    print '''CREATE TABLE test_pk_index (
        id CHAR(200) PRIMARY KEY,
        value VARCHAR(20)
    );
    '''

    num_rows = 20000

ids = range(num_rows)
random.shuffle(ids)        # For random order
# ids.reverse()            # For decreasing order

def gen_string():
    ''' Generates a random string to go into each row being added. '''

    length = random.randint(5, 20)
    letters = list(string.ascii_letters + string.digits)
    s = random.sample(letters, length)
    s = ''.join(s)
    return s

inserted = []
freq = 0.25

for id in ids:
    if mode == 'small':
        print "INSERT INTO test_pk_index VALUES (%d, '%s');" % (id, gen_string())
    elif mode in ['large', 'huge']:
        print "INSERT INTO test_pk_index VALUES ('%d', '%s');" % (id, gen_string())

    inserted.append(id)

    # To include deletion as well, leave this code uncommented.
    while len(inserted) > 100 and random.random() < freq:
        id_to_del = random.choice(inserted)
        inserted.remove(id_to_del)

        if mode == 'small':
            print "DELETE FROM test_pk_index WHERE id=%d;" % id_to_del
        elif mode in ['large', 'huge']:
            print "DELETE FROM test_pk_index WHERE id='%d';" % id_to_del

print "\nQUIT;"

