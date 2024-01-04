import pymysql
from mysql.connector import connect
import csv
import pandas as pd
from tabulate import tabulate

# Problem 1 (5 pt.)
def initialize_database():
    # YOUR CODE GOES HERE

    # MYSQL 연결
    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8')

    with connection.cursor(dictionary=True) as cursor:
        # movies table이 없을 시 생성
        create_movie_query = '''
                CREATE TABLE IF NOT EXISTS movies (
                    movie_id INT AUTO_INCREMENT PRIMARY KEY,        
                    title VARCHAR(255),
                    director VARCHAR(255),
                    price DECIMAL(10, 2) CHECK (price>=0 AND price<=100000)
                )
            '''

        # users table이 없을 시 생성
        create_user_query = '''
                        CREATE TABLE IF NOT EXISTS users (
                            user_id INT AUTO_INCREMENT PRIMARY KEY,
                            user_name VARCHAR(255),
                            age INT CHECK (age >= 12 AND age <= 110)
                        )
                    '''

        # reservation table이 없을 시 생성
        create_reservation_query = '''
                    CREATE TABLE IF NOT EXISTS reservation (
                        reservation_id INT AUTO_INCREMENT PRIMARY KEY,
                        user_id INT,
                        movie_id INT,
                        rating INT CHECK ((rating >= 1 AND rating <= 5)),
                        FOREIGN KEY (user_id) REFERENCES users(user_id),
                        FOREIGN KEY (movie_id) REFERENCES movies(movie_id)
                    )
                '''

        cursor.execute(create_movie_query)
        cursor.execute(create_user_query)
        cursor.execute(create_reservation_query)


        # read CSV file
        df = pd.read_csv('data.csv')

        # insert data into the movies table
        for title, group in df.groupby('title'):

            insert_movie_query = '''
                        INSERT INTO movies (title, director, price)
                        VALUES (%s, %s, %s)
                    '''

            data = (title, str(group['director'].iloc[0]), float(group['price'].iloc[0]))
            cursor.execute(insert_movie_query, data)

        # insert data into the users table
        for index, row in df.iterrows():
            insert_user_query = '''
                    INSERT INTO users (user_name, age)
                    VALUES (%s, %s)
                '''

            user_data = (row['name'], int(row['age']))
            cursor.execute(insert_user_query, user_data)


        # 중복되지 않는 (name, age) set과 title 추출
        unique_user_movie_sets = df[['name', 'age', 'title']].drop_duplicates()

        # insert into reservation table
        for index, row in unique_user_movie_sets.iterrows():
            # 해당 (name, age) 로 retrieve user_id
            cursor.execute("SELECT user_id FROM users WHERE user_name = %s AND age = %s",
                               (row['name'], int(row['age'])))
            user_id_result = cursor.fetchone()

            # 해당 title 로 retrieve movie_id
            cursor.execute("SELECT movie_id FROM movies WHERE title = %s",
                               (row['title'],))
            movie_id_result = cursor.fetchone()

            # user_id 와 movie_id 존재 확인
            if user_id_result is not None and movie_id_result is not None:
                user_id = user_id_result['user_id']
                movie_id = movie_id_result['movie_id']

                # insert
                insert_relation_query = '''
                    INSERT INTO reservation (user_id, movie_id)
                    VALUES (%s, %s)
                '''
                relation_data = (user_id, movie_id)
                cursor.execute(insert_relation_query, relation_data)

        connection.commit()

    # Close connection
    connection.close()

    print('Database successfully initialized')
    # YOUR CODE GOES HERE

# Problem 15 (5 pt.)
def reset():
    # YOUR CODE GOES HERE

    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8')

    # table 있는지 확인
    # 있으면 재확인 후 삭제
    # 없거나 삭제 했으면 #1 initialize() 진행

    # 각 movies, users, reservation table의 존재 여부를 확인하는 변수
    m = u = r = 0
    with connection.cursor(dictionary=True) as cursor:
        cursor.execute("SHOW TABLES like 'movies'")
        result = cursor.fetchall()
        if len(result) == 1: m = 1
        cursor.execute("SHOW TABLES like 'users'")
        result = cursor.fetchall()
        if len(result) == 1: u = 1
        cursor.execute("SHOW TABLES like 'reservation'")
        result = cursor.fetchall()
        if len(result) == 1: r = 1

        if m == 1 or u == 1 or r == 1:
            print("정말 삭제하시겠습니다? y/n: ")
            if input() == 'y':
                #삭제
                #reservation table 을 우선 삭제
                if r == 1:
                    cursor.execute("DROP TABLE reservation")
                if m == 1:
                    cursor.execute("DROP TABLE movies")
                if u == 1:
                    cursor.execute("DROP TABLE users")

        elif len(result) == 0: pass

    connection.commit()

    #go to 1
    initialize_database()

    # YOUR CODE GOES HERE

# Problem 2 (4 pt.)
def print_movies():
    # YOUR CODE GOES HERE

    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8'
    )

    # select id title director price from movie table
    # reservation, avg_rating 은 reservation table 을 join 함으로써 계산
    with connection.cursor(dictionary=True) as cursor:
        print_movie_query = '''
            SELECT
                m.movie_id,
                m.title,
                m.director,
                m.price,
                IFNULL(COUNT(r.movie_id), 0) as reservation,
                AVG(r.rating) as avg_rating
            FROM
                movies m
            LEFT JOIN
                reservation r ON m.movie_id = r.movie_id
            GROUP BY
                m.movie_id, m.title, m.director, m.price
            ORDER BY
                m.movie_id

        '''
        cursor.execute(print_movie_query)
        records = cursor.fetchall()
        for record in records:
            for key, value in record.items():
                if value == "" or value is None:
                    record[key] = 'None'
        print(tabulate(records, headers='keys', tablefmt='psql'))
    
    # YOUR CODE GOES HERE

# Problem 3 (4 pt.)
def print_users():
    # YOUR CODE GOES HERE
    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8'
    )
    # select id name age from user table
    with connection.cursor(dictionary=True) as cursor:
        print_user_query = ('SELECT user_id, user_name, age '
                            'FROM users '
                            'ORDER BY user_id')
        cursor.execute(print_user_query)
        records = cursor.fetchall()
        for record in records:
            for key, value in record.items():
                if value == "" or value is None:
                    record[key] = 'None'
        print(tabulate(records, headers='keys', tablefmt='psql'))

    # YOUR CODE GOES HERE

# Problem 4 (4 pt.)
def insert_movie():
    # YOUR CODE GOES HERE
    title = input('Movie title: ')
    director = input('Movie director: ')
    price = input('Movie Price: ')

    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8'
    )
    #error 존재 여부 판단
    error = 0
    with connection.cursor(dictionary=True) as cursor:
        select_movie_query = ('SELECT title FROM movies')
        cursor.execute(select_movie_query)
        records = cursor.fetchall()
        result = []
        for record in records:
            result += [record.get('title')]

        if title in result:
            print(f'Movie {title} already exists')
            error = 1
        if int(price)<0 or int(price)>100000:
            print('Movie price should be from 0 to 100000')
            error = 1

        #error 없으면 insert
        if error == 0:
            #reservation 찾기
            insert_movie_query = (
                'INSERT INTO movies (title, director, price) '
                'VALUES (%s, %s, %s)'
            )
            cursor.execute(insert_movie_query, (title, director, price))
            print('One movie successfully inserted')
    connection.commit()

    # YOUR CODE GOES HERE

# Problem 5 (4 pt.)
def remove_movie():
    # YOUR CODE GOES HERE

    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8'
    )
    movie_id = input('Movie ID: ')
    with connection.cursor(dictionary=True) as cursor:
        # Check if the movie_id exists in the movies table
        cursor.execute("SELECT * FROM movies WHERE movie_id = %s", (movie_id,))
        movie_exists = cursor.fetchone()

        if movie_exists:
            # Delete records from the 'reservation' table for the specified movie_id
            delete_reservation_query = "DELETE FROM reservation WHERE movie_id = %s"
            cursor.execute(delete_reservation_query, (movie_id,))

            # Delete the movie record from the 'movies' table
            delete_movie_query = "DELETE FROM movies WHERE movie_id = %s"
            cursor.execute(delete_movie_query, (movie_id,))

            print('One movie successfully removed')
        else:
            print(f"Movie {movie_id} does not exist.")

        connection.commit()

    # YOUR CODE GOES HERE

# Problem 6 (4 pt.)
def insert_user():
    # YOUR CODE GOES HERE
    name = input('User name: ')
    age = input('User age: ')

    # Check if age is within the specified bounds
    if not (12 <= int(age) <= 110):
        print('User age should be from 12 to 110')
        return

    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8'
    )

    with connection.cursor(dictionary=True) as cursor:
        # Check if the user already exists
        cursor.execute("SELECT * FROM users WHERE user_name = %s AND age = %s", (name, age))
        existing_user = cursor.fetchone()

        if existing_user:
            print(f'User ({name}, {age}) already exists')
        else:
            # Insert the user into the users table
            insert_user_query = "INSERT INTO users (user_name, age) VALUES (%s, %s)"
            cursor.execute(insert_user_query, (name, age))
            connection.commit()

            print('One user successfully inserted')

    connection.close()
    # YOUR CODE GOES HERE

# Problem 7 (4 pt.)
def remove_user():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')

    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8'
    )

    with connection.cursor(dictionary=True) as cursor:
        # Check if the user with the specified user_id exists
        cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        existing_user = cursor.fetchone()

        if existing_user:
            # Delete records from the 'reservation' table for the specified user_id
            delete_reservation_query = "DELETE FROM reservation WHERE user_id = %s"
            cursor.execute(delete_reservation_query, (user_id,))

            # Delete the user record from the 'users' table
            delete_user_query = "DELETE FROM users WHERE user_id = %s"
            cursor.execute(delete_user_query, (user_id,))

            print(f"One user successfully removed.")
        else:
            print(f"User with ID {user_id} does not exist.")

        connection.commit()

    connection.close()

    # YOUR CODE GOES HERE

# Problem 8 (5 pt.)
def book_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')

    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8'
    )

    with connection.cursor(dictionary=True) as cursor:
        # Check if the specified user_id and movie_id exist in the users and movies tables
        cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        user_exists = cursor.fetchone()

        cursor.execute("SELECT * FROM movies WHERE movie_id = %s", (movie_id,))
        movie_exists = cursor.fetchone()

        if not user_exists or not movie_exists:
            # Print error messages and exit if user_id or movie_id does not exist
            if not user_exists:
                print(f"User {user_id} does not exist.")
            if not movie_exists:
                print(f"Movie {movie_id} does not exist.")
            return

        # Check if the user has already booked the movie
        cursor.execute("SELECT * FROM reservation WHERE user_id = %s AND movie_id = %s", (user_id, movie_id))
        existing_reservation = cursor.fetchone()

        if existing_reservation:
            print(f"User {user_id} already booked movie {movie_id}.")
            return

        # Check if the movie has already been fully booked (more than 10 reservations)
        cursor.execute("SELECT COUNT(*) as reservation_count FROM reservation WHERE movie_id = %s", (movie_id,))
        reservation_count = cursor.fetchone()['reservation_count']

        if reservation_count >= 10:
            print(f"Movie {movie_id} has already been fully booked.")
            return

        # Insert a new reservation record into the reservation table
        insert_reservation_query = "INSERT INTO reservation (user_id, movie_id) VALUES (%s, %s)"
        cursor.execute(insert_reservation_query, (user_id, movie_id))

        print('Movie successfully booked')

        connection.commit()

    connection.close()
    # YOUR CODE GOES HERE

# Problem 9 (5 pt.)
def rate_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')
    user_id = input('User ID: ')
    rating = input('Ratings (1~5): ')

    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8'
    )

    with connection.cursor(dictionary=True) as cursor:
        # Check if the specified user_id and movie_id exist in the users and movies tables
        cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        user_exists = cursor.fetchone()

        cursor.execute("SELECT * FROM movies WHERE movie_id = %s", (movie_id,))
        movie_exists = cursor.fetchone()

        if not user_exists or not movie_exists:
            # Print error messages and exit if user_id or movie_id does not exist
            if not user_exists:
                print(f"User {user_id} does not exist.")
            if not movie_exists:
                print(f"Movie {movie_id} does not exist.")
            return

        # Check if the user has booked the movie by verifying the existence of the user-movie pair in the reservation table
        cursor.execute("SELECT * FROM reservation WHERE user_id = %s AND movie_id = %s", (user_id, movie_id))
        reservation_info = cursor.fetchone()

        if not reservation_info:
            print(f"User {user_id} has not booked movie {movie_id} yet.")
            return

        # Check if the user has already rated the movie by verifying if the rating is None for the user-movie pair
        if reservation_info['rating'] is not None:
            print(f"User {user_id} has already rated movie {movie_id}.")
            return

        # Check if the provided rating is within the valid range (1 to 5)
        if not (1 <= int(rating) <= 5):
            print(f"Wrong value for a rating. Please provide a rating between 1 and 5.")
            return

        # Update the rating for the user-movie pair in the reservation table
        update_rating_query = "UPDATE reservation SET rating = %s WHERE user_id = %s AND movie_id = %s"
        cursor.execute(update_rating_query, (int(rating), user_id, movie_id))

        print('Movie successfully rated')

        connection.commit()

    connection.close()
    # YOUR CODE GOES HERE


# Problem 10 (5 pt.)
def print_users_for_movie():
    # YOUR CODE GOES HERE
    movie_id = input('Movie ID: ')

    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8'
    )

    with connection.cursor(dictionary=True) as cursor:
        # Check if the movie_id exists
        cursor.execute("SELECT * FROM movies WHERE movie_id = %s", (movie_id,))
        movie_exists = cursor.fetchone()

        if not movie_exists:
            print(f'Movie {movie_id} does not exist')
        else:
            # get users
            query = '''
                SELECT
                    u.user_id,
                    u.user_name,
                    u.age,
                    r.rating
                FROM
                    users u
                JOIN
                    reservation r ON u.user_id = r.user_id
                WHERE
                    r.movie_id = %s
                ORDER BY
                    u.user_id;
            '''
            cursor.execute(query, (movie_id,))
            records = cursor.fetchall()
            for record in records:
                for key, value in record.items():
                    if value == "" or value is None:
                        record[key] = 'None'
            print(tabulate(records, headers='keys', tablefmt='psql'))

    # YOUR CODE GOES HERE

# Problem 11 (5 pt.)
def print_movies_for_user():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')

    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8'
    )

    with connection.cursor(dictionary=True) as cursor:
        # Check if the user_id exists
        cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        user_exists = cursor.fetchone()

        if not user_exists:
            print(f'User {user_id} does not exist')
        else:
            # get movies
            query = '''
                SELECT
                    m.movie_id,
                    m.title,
                    m.director,
                    m.price,
                    r.rating
                FROM
                    movies m
                LEFT JOIN
                    reservation r ON m.movie_id = r.movie_id
                WHERE
                    r.user_id = %s
                ORDER BY
                    m.movie_id;
            '''
            cursor.execute(query, (user_id,))
            records = cursor.fetchall()
            for record in records:
                for key, value in record.items():
                    if value == "" or value is None:
                        record[key] = 'None'
            print(tabulate(records, headers='keys', tablefmt='psql'))

    # Close connection
    connection.close()
    # YOUR CODE GOES HERE

# Problem 12 (6 pt.)
def recommend_popularity():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')

    connection = connect(
        host='astronaut.snu.ac.kr',
        port=7000,
        user='DB2020_11187',
        password='DB2020_11187',
        db='DB2020_11187',
        charset='utf8'
    )

    with connection.cursor(dictionary=True) as cursor:
        # Check if the user_id exists
        cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        user_exists = cursor.fetchone()

        if not user_exists:
            print(f'User {user_id} does not exist')
        else:
            # recommend movies
            query = '''
                        SELECT
                            m.movie_id,
                            m.title,
                            m.director,
                            m.price,
                            COUNT(r.movie_id) AS reservation,
                            AVG(r.rating) AS avg_rating
                        FROM
                            movies m
                        LEFT JOIN
                            reservation r ON m.movie_id = r.movie_id
                        WHERE
                            m.movie_id NOT IN (
                                SELECT r.movie_id FROM reservation r WHERE r.user_id = %s
                                )
                        GROUP BY
                            m.movie_id, m.title, m.director, m.price
                        ORDER BY
                            reservation DESC, m.movie_id ASC
                        LIMIT 2;
                    '''
            cursor.execute(query, (user_id,))
            records1 = cursor.fetchall()

    #
    with connection.cursor(dictionary=True) as cursor:
        # Check if the user_id exists
        cursor.execute("SELECT * FROM users WHERE user_id = %s", (user_id,))
        user_exists = cursor.fetchone()

        if not user_exists:
            print(f'User {user_id} does not exist')
        else:
            # recommend movies
            query = '''
                        SELECT
                            m.movie_id,
                            m.title,
                            m.director,
                            m.price,
                            COUNT(r.movie_id) AS reservation,
                            AVG(r.rating) AS avg_rating
                        FROM
                            movies m
                        LEFT JOIN
                            reservation r ON m.movie_id = r.movie_id 
                        WHERE
                            m.movie_id NOT IN (
                                SELECT r.movie_id FROM reservation r WHERE r.user_id = %s
                            )
                        GROUP BY
                            m.movie_id, m.title, m.director, m.price
                        ORDER BY
                            avg_rating DESC, m.movie_id ASC
                        LIMIT 2;
                    '''
            cursor.execute(query, (user_id, ))
            records2 = cursor.fetchall()

            for record in records1:
                for key, value in record.items():
                    if value == "" or value is None:
                        record[key] = 'None'

            for record in records2:
                for key, value in record.items():
                    if value == "" or value is None:
                        record[key] = 'None'

            # 출력 순서는 평균 평점이 가장 높은 영화, 가장 많은 고객이 본 영화 순이다.
            for r1, r2 in zip(records1, records2):
                if r1['movie_id'] == r2['movie_id']:
                    result = [r1, records2[1]]
                    print(tabulate(result, headers='keys', tablefmt='psql'))
                    break;
                else:
                    result = [r1, r2]
                    print(tabulate(result, headers='keys', tablefmt='psql'))
                    break;

    # Close connection
    connection.close()

    # YOUR CODE GOES HERE

# Problem 13 (10 pt.)
def recommend_item_based():
    # YOUR CODE GOES HERE
    user_id = input('User ID: ')

    # error message
    print(f'User {user_id} does not exist')
    print('Rating does not exist')
    # YOUR CODE GOES HERE
    pass


# Total of 70 pt.
def main():
    while True:
        print('============================================================')
        print('1. initialize database')
        print('2. print all movies')
        print('3. print all users')
        print('4. insert a new movie')
        print('5. remove a movie')
        print('6. insert a new user')
        print('7. remove an user')
        print('8. book a movie')
        print('9. rate a movie')
        print('10. print all users who booked for a movie')
        print('11. print all movies booked by an user')
        print('12. recommend a movie for a user using popularity-based method')
        print('13. recommend a movie for a user using user-based collaborative filtering')
        print('14. exit')
        print('15. reset database')
        print('============================================================')
        menu = int(input('Select your action: '))

        if menu == 1:
            initialize_database()
        elif menu == 2:
            print_movies()
        elif menu == 3:
            print_users()
        elif menu == 4:
            insert_movie()
        elif menu == 5:
            remove_movie()
        elif menu == 6:
            insert_user()
        elif menu == 7:
            remove_user()
        elif menu == 8:
            book_movie()
        elif menu == 9:
            rate_movie()
        elif menu == 10:
            print_users_for_movie()
        elif menu == 11:
            print_movies_for_user()
        elif menu == 12:
            recommend_popularity()
        elif menu == 13:
            recommend_item_based()
        elif menu == 14:
            print('Bye!')
            break
        elif menu == 15:
            reset()
        else:
            print('Invalid action')


if __name__ == "__main__":
    main()