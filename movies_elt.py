import sqlite3, csv

class MovieDatabase:
    def __init__(self, file_name, db_name="movies2.db"):
        self.file_name = file_name
        self.db_name = db_name
        self.movies_list = []
        self.columns_clean = None
        self.columns = None
        
    def extract_data(self):
        with open(self.file_name, "r") as read:
            reading = csv.DictReader(read)
            for movie in reading:
                self.movies_list.append(movie)
                
    def clean_column_names(self):
        movie_columns = list(self.movies_list[0].keys())
        
        columns_list = [movie_columns[i] 
                for i in range(len(movie_columns))
                if movie_columns[i] != ""]
        
        self.columns_clean = ["_".join(columns_list[i].\
                         split(" ")).\
                         split("(in_$)")[0].strip("_")
                         for i in range(len(columns_list))]
        
        return self.columns_clean
    
    def create_table(self):
        datatypes = ["TEXT NOT NULL UNIQUE",
                    "TEXT NOT NULL UNIQUE",
                    "TEXT NOT NULL", 
                    "TEXT NOT NULL",
                    "INTEGER NOT NULL",
                    "INTEGER NOT NULL",
                    "INTEGER NOT NULL",
                    "TEXT NOT NULL",
                    "TEXT NOT NULL",
                    "TEXT NOT NULL"]
        
        self.columns = [self.columns_clean[i] + " " + datatypes[i]
                       for i in range(len(datatypes))]
        
        sql_statement = """CREATE TABLE IF NOT EXISTS
        Movies (id INTEGER PRIMARY KEY, """  +  str(self.columns).\
        replace("[","").replace("]","").\
        replace("'","") + ")"

        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute(sql_statement)
        conn.commit()
        conn.close()
    
    def load_data(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        for movie in self.movies_list:
            title = movie["Title"]
            movie_info = movie["Movie Info"]
            distributor = movie['Distributor']
            release_date = movie['Release Date']
            dom_sales = int(movie['Domestic Sales (in $)'])
            inter_sales = int(movie['International Sales (in $)'])
            world_sales = int(movie['World Sales (in $)'])
            genre = movie['Genre']
            movie_runtime = movie['Movie Runtime']
            license = movie['License']
            insert_query = """
            INSERT INTO Movies (Title, Movie_Info, Distributor,
                                Release_Date, Domestic_Sales,
                                International_Sales,
                                World_Sales, Genre, Movie_Runtime,
                                License)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            cursor.execute(insert_query,
                           (title, movie_info, distributor,
                            release_date, dom_sales,
                            inter_sales, world_sales,
                            genre, movie_runtime, license))
        conn.commit()
        conn.close()
        
    def transform_sales_millions(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("""
        UPDATE Movies SET
        Domestic_Sales = Domestic_Sales / 1000000,
        International_Sales = International_Sales / 1000000,
        World_Sales = World_Sales / 1000000
        """)
        conn.commit()
        conn.close()

    def transform_genre(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("""UPDATE Movies 
        SET Genre = REPLACE(Genre, '[', '')""")
        cursor.execute("""UPDATE Movies 
        SET Genre = REPLACE(Genre, ']', '')""")
        cursor.execute("""UPDATE Movies 
        SET Genre = REPLACE(Genre, ' ', '')""")
        cursor.execute("""UPDATE Movies 
        SET Genre = REPLACE(Genre, '''', '')""")
        conn.commit()
        conn.close()
    
    def transform_sales_level(self):
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        # Add Sales_Level column
        cursor.execute("""ALTER TABLE Movies ADD COLUMN 
        Sales_Level TEXT""")
        # Update Sales_Level based on Total_Sales
        cursor.execute("""
        UPDATE Movies SET Sales_Level =
        CASE WHEN World_Sales <= 500 THEN 'Low Sales'
             WHEN World_Sales > 500 AND World_Sales <= 2000
             THEN 'Medium Sales'
             ELSE 'High Sales' END;

        """)
        conn.commit()
        conn.close()
        
if __name__ == "__main__":
    # Instance of MovieDatabase class
    db = MovieDatabase("Highest Hollywood Grossing Movies.csv")
    db.extract_data()
    db.clean_column_names()
    db.create_table()
    db.load_data()
    db.transform_sales_millions()
    db.transform_genre()
    db.transform_sales_level()

