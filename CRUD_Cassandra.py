from cassandra.cluster import Cluster

# Create a connection to the cluster
def create_connection():
    cluster = Cluster(['127.0.0.1'], port=2020)  # Replace with your contact point
    return cluster.connect('data')  # Replace 'demo' with your keyspace

# Insert a new record
def create_user(session, tconst, averagerating, genres, isadult, numvotes, originaltitle, primarytitle, runtimeminues, startyear, titletype):
    session.execute("INSERT INTO imdb (tconst, averagerating, genres, isadult, numvotes, originaltitle, primarytitle, runtimeminues, startyear, titletype) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", [tconst, averagerating, genres, isadult, numvotes, originaltitle, primarytitle, runtimeminues, startyear, titletype])

# Retrieve a record
def get_movie(session, tconst):
    result = session.execute("SELECT * FROM imdb WHERE primarytitle = %s ALLOW FILTERING", [tconst]).one()
    # if result:
    #     print(result.firstname, result.age)
    for row in result:
        print(row)

def get_everything(session):
    rows = session.execute("SELECT * FROM imdb")
    for row in rows:
        # Process the row
        print(row)

# Update a record
def update_user(session, new_age, lastname):
    session.execute("UPDATE users SET age = %s WHERE lastname = %s", [new_age, lastname])

# Delete a record
def delete_user(session, lastname):
    session.execute("DELETE FROM users WHERE lastname = %s", [lastname])

# Example usage
def main():
    session = create_connection()
    # get_everything(session)
    # create_user(session, "t01", 9.9, "genreTest", "0", 12345, "originalTitleTest", "primaryTitleTest", 999, 2024, "Test Insert")
    get_movie(session, "t01")

if __name__ == "__main__":
    main()
