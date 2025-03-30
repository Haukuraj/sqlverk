import psycopg as pg
from psycopg.rows import dict_row
from pathlib import Path
from configparser import ConfigParser
import bcrypt

from datetime import date

class DatabaseAPI:
    def __init__(self, db_init: Path, section='postgresql') -> None:
        """
        Constructor
        """
        cfg = self.__config(db_init, section)
        self.main_role = cfg['user']
        self.conn_string = "host=%(host)s dbname=%(database)s user=%(user)s password=%(password)s" % (cfg)
        try:
            self.connection = pg.connect(self.conn_string, autocommit=True, row_factory=dict_row)
        except Exception as e:
            self.connection = None
            raise e
        

    def __del__(self) -> None:
        """
        Destructor. Ensures the connection gets closed.
        """
        if self.connection is not None:
            self.connection.close()


    def __config(self, file: Path, section='postgresql') -> dict:
        """
        Read the values from a database config file that has a section [postgresql]

        Parameters
        ----------
        file: Path
            The database config file in the following format:
                [section]
                host=
                database=
                user=
                password=
        section: str
            The section name in the config file.
            Default = 'postgresql'
        Returns
        -------
        dict
            Dictionary where key-value matches the config file
        """
        # create a parser
        parser = ConfigParser()
        # read config file
        parser.read(file)
        # get section, default to postgresql
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception('Section {0} not found in the {1} file'.format(section, file))
        return db


    def open(self) -> None:
        """
        Opens a new connection.
        
        If a previous connection was established it is closed before opening the new connection.
        """
        if self.connection is not None:
            self.connection.close()
        self.connection = pg.connect(self.conn_string)


    def close(self) -> None:
        """
        Closes the connection to the database.
        """
        self.connection.close()


    def __get_current_role__(self) -> str:
        """
        Get current database role

        Returns
        -------
        str:
            Returns the current role used in the database.
        """
        try:
            return self.connection.execute("SELECT current_user AS user;").fetchone()['user']
        except Exception as e:
            print(e)
            raise e

    def check_user_credentials(self, username:str, password:str) -> str:
        """
        Check a user's login information.

        If a user with the given username exists, check if the password matches the stored hashed password with bcrypt.

        Parameters
        ----------
        username: str
        password: str

        Returns
        -------
        str
            The role of the user or None if they do not exist or the password is incorrect.
        """
        try:
            user = self.connection.execute("SELECT * FROM Users WHERE username=%s", [username]).fetchone()
            if bcrypt.checkpw(password.encode('utf-8'), user['password_hashed'].encode('utf-8')):
                return user['role_name']
        except Exception as e:
            print(e)
            return None


    def retrieve_all_sports(self) -> list[dict_row]:
        """
        Get all rows from the Sports table.

        Returns
        -------
        list[dict_row]
            A list of rows, where each row is a dict with the table columns as keys.
        """
        try:
            rows = self.connection.execute("SELECT * FROM Sports").fetchall()
            return rows
        except Exception as e:
            print(e)
            raise e


    def delete_sport(self, username: str, sport:str) -> None:
        """
        Delete a given sport from the Sports table

        Parameters
        ----------
        username: str
            The user attempting to delete a sport row
        sport: str
            The name of the sport
        """
        # TODO: Task 4
        try:
            # Check if the user exists and their role
            role_query = "SELECT role_name FROM Users WHERE username=%s"
            role_result = self.connection.execute(role_query, (username,)).fetchone()

            if role_result is None:
                raise Exception("Username not found")

            role = role_result[0]

            # Check if user has the correct role
            if role not in ["editor", "theone"]:
                raise PermissionError("You do not have permission to delete a sport.")
            
                    # Delete related records from results table first
            delete_results_query = """
                DELETE FROM results WHERE sport_id = (
                    SELECT id FROM sports WHERE name=%s
                )
                """
            self.connection.execute(delete_results_query, (sport,))

            # Perform the deletion
            delete_query = "DELETE FROM Sports WHERE name=%s"
            self.connection.execute(delete_query, (sport,))
            self.connection.commit()
        except Exception as e:
            print(e)
            raise e


    def retrieve_athletes_page(self, page: int, items_per_page: int, sort_by: dict | None=None) -> tuple[list[dict_row], int]:
        """
        Get paginated rows from the Athletes table.

        Parameters
        ----------
        page: int
            Current page number (1-indexed)
        items_per_page: int
            Number of items to return
        sort_by: dict | None
            A dict in the following format: {'key': column_name, 'order': 'asc' | 'desc'}

        Returns
        -------
        tuple[list[dict_row], int]
            A tuple with the list of rows for the page and the amount of rows there are in total in the table
        """
        try:
            query = "SELECT * FROM Athletes"
            start = (page-1) * items_per_page
            end = start + items_per_page
            rows = []
            if sort_by is not None:
                columns = set(['id', 'name', 'gender', 'height'])
                key = sort_by['key']
                if key not in columns:
                    raise pg.errors.DataError(f'The provided sort key does not match any columns of the Athletes table! Key: {key}')
                if sort_by['order'] == 'desc':
                    query += f" ORDER BY {key} DESC" 
                else:
                    query += f" ORDER BY {key}" 
            
            # NOTE: Python fetchall Solution
            # Downside of this is that it retrieves all rows
            # query += f" LIMIT {items_per_page} OFFSET {start}"
            # rows = self.connection.execute(query).fetchall()
            # return (rows[start:end], len(rows))

            # NOTE: SQL LIMIT + OFFSET Solution
            # Downside of this that you have to execute an additional query to get total rows
            # query += f" LIMIT {items_per_page} OFFSET {start}" 
            # total = self.connection.execute("SELECT COUNT(*) FROM Athletes").fetchone()['count']
            # rows = self.connection.execute(query).fetchall()
            # return (rows, total)

            # NOTE: Python fetchmany Solution
            with self.connection.execute(query) as cur:
                total = cur.rowcount # Get number of effected rows
                if start != 0:
                    cur.fetchmany(start) # Offset
                rows = cur.fetchmany(items_per_page) # Limit
            return (rows, total)
        except Exception as e:
            print(e)
            raise e

    def add_athlete(self, username:str, name: str, gender: str, height: float) -> None:
        """
        Add a new athlete to the Athletes table. Uses INSERT INTO

        Parameters
        ----------
        username: str
        name: str
            Full name of the athlete
        gender: str
            Gender of the athlete
        height: float
            Height of the athlete
        """
        # TODO: Task 2 (SQL Query)
        query = """
            INSERT INTO Athletes (name, height, gender)
            VALUES (%s, %s, %s)
                """
        try:
            role_query = "SELECT role_name FROM Users WHERE username=%s"
            role_result = self.connection.execute(role_query, (username,)).fetchone()

            if role_result is None:
                raise Exception("Username not found")

            role = role_result['role_name']

            if role not in ["editor", "theone"]:
                raise PermissionError("You do not have permission to add athletes.")

            # Insert athlete into database
            query = """
                INSERT INTO Athletes (name, height, gender)
                VALUES (%s, %s, %s)
            """
            self.connection.execute(query, (name, height, gender))
            self.connection.commit()
        except Exception as e:
            print(e)
            raise e


    def add_athlete_sql_function(self, username:str, name:str, gender: str, height: float) -> int:
        """
        Add a new athlete to the Athletes table. Uses NewAthlete SQL function

        Parameters
        ----------
        username: str
        name: str
            Full name of the athlete
        gender: str
            Gender of the athlete
        height: float
            Height of the athlete
        
        Returns
        -------
        str:
            ID of the newly inserted row
        """
        # TODO: Task 2 (SQL Function)
        query = """
                SELECT Insert_athlete(%s, %s, %s, %s)
                """
        try:
            result = self.connection.execute(query, (username, name, height, gender)).fetchone()
            self.connection.commit()
            return result[0]
    
        except Exception as e:
            print(e)
            raise e
 

    def retrieve_competitions_from_place_page(self, place:str, page: int, items_per_page: int, sort_by: dict | None=None) -> tuple[list[dict_row], int]:
        """
        Get paginated rows from the Competitions table.

        Parameters
        ----------
        page: int
            Current page number (1-indexed)
        items_per_page: int
            Number of items to return
        sort_by: dict | None
            A dict in the following format: {'key': column_name, 'order': 'asc' | 'desc'}

        Returns
        -------
        tuple[list[dict_row], int]
            A tuple with the list of rows for the page and the amount of rows there are in total in the table
        """
        try:
            start = (page-1) * items_per_page
            end = start + items_per_page
            rows = []
            if sort_by is not None:
                columns = set(['id', 'place', 'held'])
                key = sort_by['key']
                if key not in columns:
                    raise pg.errors.DataError(f'The provided sort key does not match any columns of the Competitions table! Key: {key}')
                if sort_by['order'] == 'desc':
                    rows = self.connection.execute(f"SELECT * FROM Competitions WHERE place=%s ORDER BY {key} DESC", [place]).fetchall()
                else:
                    rows = self.connection.execute(f"SELECT * FROM Competitions WHERE place=%s ORDER BY {key}", [place]).fetchall()
            else:
                rows = self.connection.execute("SELECT * FROM Competitions WHERE place=%s", [place]).fetchall()
            return (rows[start:end], len(rows))
        except Exception as e:
            print(e)
            raise e


    def retrieve_competition_places(self) -> list[str]:
        """
        Return all distinct places

        Returns
        -------
        list[str]
            List of places
        """
        try:
            rows = self.connection.execute("SELECT DISTINCT place FROM Competitions").fetchall()
            return [p['place'] for p in rows]
        except Exception as e:
            print(e)
            raise e


    def add_competition(self, username:str, place: str, held: str | None=None) -> int:
        """
        Add a new competition to the Competitions table.

        Parameters
        ----------
        username: str
        place: str
            Name of the place where the competition is held
        held: str | None
            Date when the competition is held - Format YYYY-MM-DD

        
        Returns
        -------
        str:
            ID of the newly inserted row

        """
        # TODO: Task 3: Add competition using an SQL Function
        query = """
                INSERT INTO Competitions (place, held)
                VALUES (%s, %s)
                RETURNING id
                """
        try:
            
            role_query = "SELECT role_name FROM Users WHERE username=%s"
            role_result = self.connection.execute(role_query, (username,)).fetchone()

            # Checking if there is a username
            if role_result is None:
                raise Exception("no username found")

            role = role_result['role_name']

            if role not in ["editor", "theone"]:
                raise PermissionError("You do not have permission to add competitions.")

            # Validating date if it is later than 2024
            competition_year = int(held.split("-")[0])
            if competition_year < 2024:
                raise ValueError("Competitions have to be held after 2024.")

            result = self.connection.execute(query, (place, held)).fetchone()
            self.connection.commit()
            return result['id']
        except Exception as e:
            print(e)
            raise e 


    def retrieve_all_results(self) -> list[dict_row]:
        """
        Get all rows from the Results table.

        Returns
        -------
        list[dict_row]
            A list of rows, where each row is a dict with the table columns as keys.
        """
        try:
            rows = self.connection.execute("SELECT * FROM Results").fetchall()
            return rows
        except Exception as e:
            print(e)
            raise e
    

    def retrieve_results_from_sports_and_places_page(self, places: list[str], sports: list[str],
                                                     page: int, items_per_page: int,
                                                     sort_by: dict | None=None) -> list[dict_row]:
        """
        Retrieve results based on specified places and sports (paginated)

        Parameters
        ----------
        places: list[str]
            List of selected places from the competitions table
        sports: list[str]
            List of selected sports from the Sports table
        page: int
            Current page number (1-indexed)
        items_per_page: int
            Number of items to return
        sort_by: dict | None
            A dict in the following format: {'key': column_name, 'order': 'asc' | 'desc'}

        Returns
        -------
        (list[dict_row], int)
            A list of rows for the page and the total number of rows.
            The format of the rows:
            [{
                place: str,
                held: date,
                sport: str,
                athleteid: str,
                name: str,
                result: float
            },...]
        """
        try:
            # TODO: Task 1
            # TODO: Remember to account for pages or if there is a sort order on a column.

            # NOTE (Hint): Dynamically construct the query string with the correct number of %s placeholders (", ".join(["%s"] * len(array))

            # TODO: We do also want to get results if only places or only sports have been specified.
            query="""
            SELECT c.place,c.held,s.name AS sport, a.id AS athleteid, a.name,r.result
            FROM Results R
            JOIN Competitions c ON r.competitionID= c.ID
            JOIN Sports s ON r.sportID = s.ID
            JOIN Athletes a ON r.athleteID = a.ID
            """
            filters=[]
            values=[]
            if places:
                filters.append("c.place=ANY(%s)")
                values.append(places)

            if sports:
                filters.append("s.name=ANY(%s)")
                values.append(sports)

            if filters:
                query +=" WHERE " + " AND ".join(filters)


            if sort_by:
                valid_keys={"place","held","sport","athleteid","name","result"}
                key=sort_by.get('key')
                order=sort_by.get('order','asc')
                if key in valid_keys and order.lower() in ['asc','desc']:
                    query += f" ORDER BY {key} {order.upper()}"

            rows= self.connection.execute(query,values).fetchall()

            start=(page-1)*items_per_page
            end=start + items_per_page
            paginated_rows=rows[start:end]
            return (paginated_rows, len(rows))
    
        except Exception as e:
            print(e)
            raise e
        #3.4
    def delete_sport(conn, username, sport_name, force_delete=False):
        """
        Deletes a sport from the database. If force_delete is False, it ensures there are no dependencies.
        Only users with the role of 'editor' or 'theone' can perform this action.
        """
        try:
            with conn.cursor() as cur:
                # Check if the user exists and retrieve their role
                cur.execute("SELECT role_name FROM Users WHERE username = %s;", (username,))
                role_result = cur.fetchone()
                
                if role_result is None:
                    print("Username not found.")
                    return
                
                role = role_result[0]
                
                # Check user role
                if role not in ["editor", "theone"]:
                    print("You do not have permission to delete a sport.")
                    return
                
                # Check if the sport exists
                cur.execute("SELECT id FROM sports WHERE name = %s;", (sport_name,))
                sport = cur.fetchone()
                if not sport:
                    print("Sport not found.")
                    return
                
                sport_id = sport[0]
                
                if not force_delete:
                    # Check if the sport has dependencies
                    cur.execute("""
                        SELECT COUNT(*) FROM events WHERE sport_id = %s
                        UNION ALL
                        SELECT COUNT(*) FROM teams WHERE sport_id = %s
                    """, (sport_id, sport_id))
                    
                    dependencies = sum(row[0] for row in cur.fetchall())
                    if dependencies > 0:
                        print("Cannot delete sport. It has associated records.")
                        return
                else:
                    # Delete dependent rows first
                    cur.execute("DELETE FROM events WHERE sport_id = %s;", (sport_id,))
                    cur.execute("DELETE FROM teams WHERE sport_id = %s;", (sport_id,))
                
                # Delete the sport
                cur.execute("DELETE FROM sports WHERE id = %s;", (sport_id,))
                conn.commit()
                print(f"Sport '{sport_name}' deleted successfully.")
        except Exception as e:
            conn.rollback()
            print("Error deleting sport:", e)
    #3.5
    def create_role_and_user(conn, role_name, username, password):
        """
        Creates a new role with specific permissions and adds a user with that role.
        """
        try:
            with conn.cursor() as cur:
                # Create the new role
                cur.execute(f"CREATE ROLE {role_name} WITH LOGIN;")
                
                # Grant permissions
                cur.execute(f"GRANT INSERT ON Competitions TO {role_name};")
                cur.execute(f"GRANT SELECT ON Athletes, Results TO {role_name};")
                
                # Hash the password
                hashed_password = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
                
                # Insert new user
                cur.execute("INSERT INTO Users (username, password_hash, role_name) VALUES (%s, %s, %s);", (username, hashed_password, role_name))
                
                conn.commit()
                print(f"Role '{role_name}' and user '{username}' created successfully.")
        except Exception as e:
            conn.rollback()
            print("Error creating role and user:", e)

        


    def retrieve_all_genders(self) -> list[dict_row]:
        """
        Get all rows from the Genders table.

        Returns
        -------
        list[dict_row]
            A list of rows, where each row is a dict with the table columns as keys.
        """
        try:
            rows = self.connection.execute("SELECT * FROM Gender").fetchall()
            return rows
        except Exception as e:
            print(e)
            raise e