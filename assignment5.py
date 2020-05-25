import csv
import sys
import mysql.connector
from mysql.connector import errorcode
#from prettytable import PrettyTable

#globals
#username/paswd from command line:
username = sys.argv[1]
password = sys.argv[2]

#globals:
file = "tmdb_5000_movies.csv"

# take from commandline:
try:
    db = mysql.connector.connect(
        user=username,
        passwd=password
        )
    mycursor = db.cursor(buffered=True)
except:
    print("err: invalid username or password.")
    print("access denied")
#as I understood from https://dev.mysql.com/
#catches error if database does not exist:

try:
    mycursor.execute("USE movies_db")

except mysql.connector.Error as err:
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        try:
            mycursor.execute("CREATE DATABASE movies_db")
            db = mysql.connector.connect(
            database="movies_db"
            )

        except mysql.connector.Error as err:
            print("Error creating database")
            exit(1)
        db.database = "movies_db"
    else:
        print(err)
        exit(1)

#-----------------
def drop2Refresh():
    #drop all tables before creating new
    #drop all foreign key dependencies first:
    sql = "DROP TABLE IF EXISTS {}".format("movie_genres")
    mycursor.execute(sql)
    sql = "DROP TABLE IF EXISTS {}".format("movie_keywords")
    mycursor.execute(sql)
    sql = "DROP TABLE IF EXISTS {}".format("movie_producers")
    mycursor.execute(sql)
    sql = "DROP TABLE IF EXISTS {}".format("movie_languages")
    mycursor.execute(sql)
    sql = "DROP TABLE IF EXISTS {}".format("movie_countries")
    mycursor.execute(sql)
    sql = "DROP TABLE IF EXISTS {}".format("production_countries")
    mycursor.execute(sql)
    sql = "DROP TABLE IF EXISTS {}".format("production_comps")
    mycursor.execute(sql)
    sql = "DROP TABLE IF EXISTS {}".format("movies")
    mycursor.execute(sql)
    sql = "DROP TABLE IF EXISTS {}".format("genres")
    mycursor.execute(sql)
    sql = "DROP TABLE IF EXISTS {}".format("keywords")
    mycursor.execute(sql)
    sql = "DROP TABLE IF EXISTS {}".format("spoken_languages")
    mycursor.execute(sql)
# end drop2Refresh


#------------------
def createTables():
    #genre, production_companies,
    #production_countries, spoken_languages
    #are in seperate tables to make 2NF

    # movies TABLE:
    try:
        sql = """CREATE TABLE movies(
                budget INT,
                homepage VARCHAR(255),
                id INT NOT NULL,
                original_language VARCHAR(6) NOT NULL,
                original_title VARCHAR(255) NOT NULL,
                overview TEXT,
                popularity DOUBLE(11,6),
                release_date DATE,
                revenue BIGINT,
                runtime INT NOT NULL,
                status VARCHAR(16) NOT NULL,
                tagline TEXT,
                title VARCHAR(255),
                vote_average DOUBLE(3,1),
                vote_count INT,
                PRIMARY KEY(id));
                """
        mycursor.execute(sql)
    except mysql.connector.Error as err:
        pass #continue if exists. handle error.

    try:
        # GENRE TABLE:
        sql= """CREATE TABLE genres(
            id INT NOT NULL,
            name VARCHAR(255),
            PRIMARY KEY(id));
            """
        mycursor.execute(sql)
    except mysql.connector.Error as err:
        pass #continue if exists. handle error.

    try:
        # MOVIE GENRE TABLE:
        sql="""CREATE TABLE movie_genres(
            id INT NOT NULL AUTO_INCREMENT,
            genre_id INT NOT NULL,
            movie_id INT NOT NULL,
            FOREIGN KEY(genre_id) REFERENCES genres(id),
            FOREIGN KEY(movie_id) REFERENCES movies(id),
            PRIMARY KEY(id));
            """
        mycursor.execute(sql)
    except mysql.connector.Error as err:
        pass #continue if exists. handle error.

    try:
        # KEYWORDS TABLE:
        sql="""CREATE TABLE keywords(
            id INT NOT NULL,
            name VARCHAR(255) NOT NULL,
            PRIMARY KEY(id));
            """
        mycursor.execute(sql)
    except mysql.connector.Error as err:
        pass #continue if exists. handle error.

    try:
        # MOVIE KEYWORDS TABLE:
        sql="""CREATE TABLE movie_keywords(
            id INT NOT NULL AUTO_INCREMENT,
            keyword_id INT NOT NULL,
            movie_id INT NOT NULL,
            PRIMARY KEY(id),
            FOREIGN KEY(keyword_id) REFERENCES keywords(id),
            FOREIGN KEY(movie_id) References movies(id));
            """
        mycursor.execute(sql)
    except mysql.connector.Error as err:
        pass #continue if exists. handle error.

    try:
        # PRODUCTION COUNTRIES TABLE:
        sql="""CREATE TABLE production_countries(
            iso_3166_1 VARCHAR(2) NOT NULL,
            name VARCHAR(255) NOT NULL,
            PRIMARY KEY(iso_3166_1))
            """
        mycursor.execute(sql)
    except mysql.connector.Error as err:
        pass #continue if exists. handle error.

    try:
        # MOVIE COUNTRIES TABLE:
        sql="""CREATE TABLE movie_countries(id INT NOT NULL AUTO_INCREMENT,
            mc_id VARCHAR(2) NOT NULL,
            movie_id INT NOT NULL,
            FOREIGN KEY(mc_id) REFERENCES production_countries(iso_3166_1),
            FOREIGN KEY(movie_id) REFERENCES movies(id),
            PRIMARY KEY(id))
            """
        mycursor.execute(sql)
    except mysql.connector.Error as err:
        pass #continue if exists. handle error.

    try:
        # PRODUCTION COMPANIES TABLE:
        sql="""CREATE TABLE production_comps(
            id INT NOT NULL,
            name VARCHAR(255),
            PRIMARY KEY(id))
            """
        mycursor.execute(sql)
    except mysql.connector.Error as err:
        pass #continue if exists. handle error.

    try:
        # MOVIE PRODUCERS TABLE:
        sql="""CREATE TABLE movie_producers(
            id INT NOT NULL AUTO_INCREMENT,
            producer_id INT NOT NULL,
            movie_id INT NOT NULL,
            FOREIGN KEY(movie_id) REFERENCES movies(id),
            FOREIGN KEY(producer_id) REFERENCES production_comps(id),
            PRIMARY KEY(id))
            """
        mycursor.execute(sql)
    except mysql.connector.Error as err:
        pass #continue if exists. handle error.

    try:
        # SPOKEN LANGUAGES TABLE:
        sql="""CREATE TABLE spoken_languages(
            iso_639_1 VARCHAR(2) NOT NULL,
            name VARCHAR(255),
            PRIMARY KEY(iso_639_1));
            """
        mycursor.execute(sql)

    except mysql.connector.Error as err:
        pass #continue if exists. handle error.

    try:
        # MOVIE LANGUAGES TABLE:
        sql="""CREATE TABLE movie_languages(
            id INT NOT NULL AUTO_INCREMENT,
            ml_id VARCHAR(2) NOT NULL,
            movie_id INT NOT NULL,
            FOREIGN KEY(ml_id) REFERENCES spoken_languages(iso_639_1),
            FOREIGN KEY(movie_id) REFERENCES movies(id),
            PRIMARY KEY(id))
            """
        mycursor.execute(sql)
    except mysql.connector.Error as err:
        pass #continue if exists. handle error.
#end of createtables

def insertData():
    #https://docs.python.org/3.4/library/csv.html?highlight=csv
    with open(file, 'r') as csvfile:
        data = csv.reader(csvfile)
        # INSERTING MOVIES:
        sql_movies = ("INSERT INTO movies "
               "(budget, homepage, id, original_language, original_title, overview, popularity, release_date, revenue, runtime, status, tagline, title, vote_average, vote_count)"
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                  #0        2      3       5                  6             7          8          11           12         13       15      16      17        18         19


        for row in data:
            try: # values to be inserted into their own tables:
                rows = (row[0], row[2], row[3], row[5], row[6], row[7], row[8], row[11], row[12], row[13], row[15], row[16], row[17], row[18], row[19])
                #grab values for intermediate tables for 2NF:
                genres =  eval(row[1])
                keyword =  eval(row[4])
                company =  eval(row[9])
                country =  eval(row[10])
                language =  eval(row[14])

                #handle incorrect values:
                if row[11] == '': # release_date
                    row[11] = None #replace with null.
                else:
                    row[11] = row[11].replace('/','-')
                if row[13] == '': #runtime
                    row[13] = 0
            except: # continue even if invalid data type in the table.
                continue
            try:
                mycursor.execute(sql_movies, rows)

            except mysql.connector.Error as err:
                continue
# I     NSERTING GENRE AND MOVIE GENRE:
        sql_genres = ("INSERT INTO genres (id, name) VALUES (%s, %s)")
        sql_movie_genres =("INSERT INTO movie_genres(genre_Id, movie_Id) VALUES (%s, %s)")
        for g in genres:
            genreRows = (g['id'], row[3]) # insert movie id.
            genreName = (g['id'], g['name'])
            try: # insert the data into genre.
                mycursor.execute(sql_genres, genreName)
            except mysql.connector.Error as err:
                print(err)
                continue
            try: # insert the data into genre.
                mycursor.execute(sql_movie_genres, genreName)
                print(err)
            except mysql.connector.Error as err:
                continue
        # end of loop

#       INSERT INTO KEYWORDS AND MOVIE KEYWORDS:
        sql_kw = ("INSERT INTO keywords(id, name) VALUES (%s, %s)")
        sql_movie_kw = ("INSERT INTO movie_keywords (keyword_id, movie_id) VALUES (%s, %s)")

        for k in keyword:
            kwRows = (k['id'], row[3]) # insert movie id.
            kwName = (k['id'], k['name'])
            try:
                mycursor.execute(sql_kw, kwName)
            except mysql.connector.Error as err:
                print(err)
                continue
            try:
                mycursor.execute(sql_movie_kw, kwRows)
            except mysql.connector.Error as err:
                print(err)
                continue
        # end of loop

#       INSERTING PRODUCTION COMPANY, PRODUCTION COUNTRIES AND MOVIE PRODUCERS
        sql_PC = ("INSERT INTO production_comps (id, name) VALUES (%s, %s)")
        sql_MP = ("INSERT INTO movie_producers (producer_id, movie_id) VALUES (%s, %s)")
        for c in company:
            cRows = (c['id'], row[3]) # insert movie id.
            cName = (c['id'], c['name'])
            try: # insert the data into genre.
                mycursor.execute(sql_PC,cName)
            except mysql.connector.Error as err:
                print(err)
                continue
            try:
                mycursor.execute(sql_MP, cRows)
            except mysql.connector.Error as err:
                print(err)
                continue
        # end of loop
#       INSERTING PRODUCTION COUNTRY, MOVIE COUNTRY
        sql_PCountry = ("INSERT INTO production_countries (iso_3166_1, name) VALUES (%s, %s)")
        sql_MovieCountry = ("INSERT INTO movie_countries (mc_id, movie_id) VALUES (%s, %s)")
        for c in country:
            cRows = (c['iso_3166_1'], row[3]) # insert movie id.
            cName = (c['iso_3166_1'], c['name'])
            try:
                mycursor.execute(sql_PCountry,cName)
            except mysql.connector.Error as err:
                print(err)
                continue
            try:
                mycursor.execute(sql_MovieCountry, cRows)
            except mysql.connector.Error as err:
                print(err)
                continue
        # end of loop
#       INSERTING LANGUAGE TABLES
        sql_SL = ("INSERT  INTO spoken_languages (iso_639_1, name) VALUES (%s, %s)")
        sql_ML = ("INSERT INTO movie_languages (ml_id, movie_id) VALUES (%s, %s)")
        for c in language:
            cRows = (c['iso_639_1'], row[3]) # insert movie id.
            cName = (c['iso_639_1'], c['name'])
            try: # insert the data into genre.
                mycursor.execute(sql_SL,cName)
            except mysql.connector.Error as err:
                print(err)
                continue
            try:
                mycursor.execute(sql_ML, cRows)
            except mysql.connector.Error as err:
                print(err)
                continue
        # end of loop

    db.commit()
# end of insertData

def queryData():
    print("1) Average Budget:")
    sql= "SELECT AVG(budget) FROM movies"
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)

    print("----------------------------")
    print("2) Movies Produces in the US:")
    print("--- TITLE , COMPANY NAME ---")
    sql= "SELECT original_title, production_comps.name FROM movies JOIN movie_producers\
           ON movie_producers.movie_id=movies.id JOIN production_comps\
           ON movie_producers.producer_id=production_comps.id JOIN movie_countries ON movie_countries.movie_id=movies.id \
           WHERE movie_countries.mc_id='US' LIMIT 5"
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)

    print("----------------------------")
    print("3) Top 5 movies that made most revenue:")
    print("--- TITLE , REVENUE ---")
    sql = "SELECT movies.original_title, revenue FROM movies ORDER BY revenue DESC LIMIT 5"
    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)


    print("----------------------------")
    print("5) Greatest Popularity:")
    sql = "SELECT title, popularity  \
          FROM movies WHERE popularity > (SELECT AVG(popularity) FROM movies) LIMIT 5"

    mycursor.execute(sql)
    myresult = mycursor.fetchall()
    for x in myresult:
        print(x)


#describeData

#-----------------
drop2Refresh()
createTables()
insertData()
print("success")
queryData()
mycursor.close()
db.close()
