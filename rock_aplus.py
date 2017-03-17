import sqlite3

import requests


from contextlib import closing


URL = 'http://streaming.telecom.by:8000/aplus_rock_128'


def get_media_title(r):
    if r.headers.get('icy-metaint'):
        metaint = int(r.headers.get('icy-metaint'))
        c = r.iter_content(chunk_size=metaint+1).next()
        meta_length = ord(c[metaint]) * 16
        if meta_length > 0:
            c = r.iter_content(chunk_size=meta_length).next()
            title = c.split("'")[1]
            return title.decode('cp1251').encode('utf-8'), c[:-1]
        return None, c[:-1]
    return None, None


def prepare_db(conn):
    conn.execute('''CREATE TABLE IF NOT EXISTS song_list
                    (ID INTEGER PRIMARY KEY AUTOINCREMENT,
                     TITLE TEXT UNIQUE NOT NULL,
                     COUNT INT NOT NULL);
                ''')


def update_db(conn, title):
    try:
        select_q = "SELECT ID, COUNT FROM song_list WHERE TITLE = ?;"
        c = conn.execute(select_q, (title, ))
        row = c.fetchone()
        if row:
            id = row[0]
            count = row[1] + 1
            conn.execute('UPDATE song_list SET COUNT = ? WHERE ID = ?;', (count, id))
        else:
            conn.execute('INSERT INTO song_list (TITLE, COUNT) VALUES(?, 1);', (title, ))

        conn.commit()
    except sqlite3.OperationalError as e:
        print e


def get_all(conn):
    cursor = conn.execute('''SELECT * FROM song_list;''')
    return cursor

if __name__ == '__main__':
    conn = sqlite3.connect('rock-aplus-rock-list.db')
    conn.text_factory = str
    prepare_db(conn)
    h = {'Icy-MetaData': 1}
    with closing(requests.get(URL, stream=True, headers=h)) as r:
        f = None
        while True:
            title, content = get_media_title(r)
            if title:
                print title
                if f and not f.closed:
                    f.close()
                f = open('music/' + title + '.mp3', 'w')
                f.write(content)
                update_db(conn, title)
            elif f:
                f.write(content)

    conn.close()
