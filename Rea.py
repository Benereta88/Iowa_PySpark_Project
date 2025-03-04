
import redis

# Connect to Redis
redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)


def set_config():
        # Set a configuration key-val pair
        key = input("Enter configuration key: ").strip()
        value = input("Enter configuration value: ").strip()
        redis_client.set(key, value)
        print(f"Configuration for '{key}' set successfully.")


def get_config():
        # Retrieve a conf key/val
        key = input("Enter configuration key to retrieve: ").strip()
        value = redis_client.get(key)
        if value is None:
            print(f"Configuration key '{key}' not found.")
            return
        print(f"Configuration [{key}]: {value}")


def delete_config():
        # Delete a conf key/val
        key = input("Enter configuration key to delete: ").strip()
        redis_client.delete(key)
        print(f"Configuration key '{key}' deleted.")


def main():
        # CL Menu
        while True:
            print("\nConfiguration Manager")
            print("1. Set configuration")
            print("2. Get configuration")
            print("3. Delete configuration")
            print("4. Exit")

            choice = input("Choose an option: ").strip()

            if choice == "1":
                set_config()
            elif choice == "2":
                get_config()
            elif choice == "3":
                delete_config()
            elif choice == "4":
                print("Exiting...")
                break
            else:
                print("Invalid option. Please try again.")


# Keep running the meny
if __name__ == "__main__":
        main()

        import redis
        import psycopg2
        from fastapi import FastAPI, HTTPException
        from pydantic import BaseModel
        import json

        # Initialize Redis client
        redis_client = redis.Redis(host="localhost", port=6379, db=0, decode_responses=True)
"""
        # Connect to PostgresSQL
        pg_conn = psycopg2.connect(
            dbname="mydatabase",
            user="mouser",
            password="mypassword",
            host="localhost",
            port=5432
        )
        pg_cursor = pg_conn.cursor()

        app = FastAPI()


        # Pydantic model for updating user data
        class UserUpdate(BaseModel):
            name: str
            email: str


        @app.get("/users/{user_id}")
        def get_user(user_id: int):
            # Fetch user data from Redis cache, otherwise from PostgreSQL

            # Cache key
            cache_key = f"user:{user_id}"

            # Check Redis first
            cached_data = redis_client.get(cache_key)
            if cached_data:
                return json.loads(cached_data)  # Return cached user data

            # If not in Redis, fetch from PostgreSQL
            pg_cursor.execute("SELECT id, name, email FROM users WHERE id = %s", (user_id,))
            user = pg_cursor.fetchone()

            if not user:
                raise HTTPException(status_code=404, detail="User not found")

            user_data = {"id": user[0], "name": user[1], "email": user[2]}

            # Store in Redis cache for next time (expire in 60 seconds)
            redis_client.setex(cache_key, 60, json.dumps(user_data))

            return user_data


        @app.post("/users/{user_id}")
        def update_user(user_id: int, user_update: UserUpdate):
            # Update user data in PostgreSQL and invalidate Redis cache

            # Update user in PostgreSQL
            pg_cursor.execute(
                "UPDATE users SET name = %s, email = %s WHERE id = %s RETURNING id",
                (user_update.name, user_update.email, user_id),
            )
            updated_user = pg_cursor.fetchone()
            pg_conn.commit()

            if not updated_user:
                raise HTTPException(status_code=404, detail="User not found")

            # Invalidate Redis cache for this user
            cache_key = f"user:{user_id}"
            redis_client.delete(cache_key)

            return {"message": f"User {user_id} updated and cache invalidated."}


        if __name__ == "__main__":
            import uvicorn

            uvicorn.run(app, host="127.0.0.1", port=8000)
"""
