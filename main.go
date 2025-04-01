package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/rs/cors" // CORS package to allow cross-origin requests
	_ "github.com/lib/pq" // PostgreSQL driver
)

// Actor represents the data structure for an actor
type Actor struct {
	ActorID        int     json:"actor_id"
	FirstName      string  json:"first_name"
	LastName       string  json:"last_name"
	Salary         float64 json:"salary"
	DepartmentName string  json:"department_name"
}

func main() {
	// Connect to the source database (Postgres)
	db, err := sql.Open("postgres", "postgresql://postgres:1234@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatal("Error connecting to the database:", err)
	}
	defer db.Close()

	// Connect to the target database (your newly created target_db)
	targetDB, err := sql.Open("postgres", "postgresql://postgres:1234@localhost:5432/target_db?sslmode=disable")
	if err != nil {
		log.Fatal("Error connecting to the target database:", err)
	}
	defer targetDB.Close()

	// Verify the connection by pinging both databases
	err = db.Ping()
	if err != nil {
		log.Fatal("Error pinging the source database:", err)
	}
	fmt.Println("Connected to the source database successfully!")

	err = targetDB.Ping()
	if err != nil {
		log.Fatal("Error pinging the target database:", err)
	}
	fmt.Println("Connected to the target database successfully!")

	// Create a new router
	mux := http.NewServeMux()

	// /actors endpoint - fetch actors, salaries, and departments from the source database
	mux.HandleFunc("/actors", func(w http.ResponseWriter, r *http.Request) {
		log.Println("Received request to /actors")

		// Fetch actors, salaries, and departments from the source database
		rows, err := db.Query(`
			SELECT 
				a.actor_id, 
				a.first_name, 
				a.last_name, 
				COALESCE(s.salary, 0) AS salary, 
				COALESCE(d.department_name, 'Unknown') AS department_name
			FROM actor a
			LEFT JOIN salaries s ON a.actor_id = s.actor_id
			LEFT JOIN departments d ON s.department_id = d.department_id
			ORDER BY a.actor_id
		`)
		if err != nil {
			log.Println("Error fetching data from source database:", err)
			http.Error(w, "Error fetching data", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Prepare the data to be returned as JSON
		var actors []Actor
		for rows.Next() {
			var actor Actor
			err := rows.Scan(&actor.ActorID, &actor.FirstName, &actor.LastName, &actor.Salary, &actor.DepartmentName)
			if err != nil {
				log.Println("Error scanning data:", err)
				http.Error(w, "Error reading data", http.StatusInternalServerError)
				return
			}

			// Append the actor data to the actors slice
			actors = append(actors, actor)
		}

		// Check if no data was found
		if len(actors) == 0 {
			log.Println("No actors found")
		}

		// Return the data as JSON
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(actors)
		if err != nil {
			log.Println("Error encoding JSON:", err)
			http.Error(w, "Error encoding data", http.StatusInternalServerError)
		}
	})

	// /sync endpoint - sync data between the source database and target database
	mux.HandleFunc("/sync", func(w http.ResponseWriter, r *http.Request) {
		log.Println("Received request to sync data")

		// Fetch data from source database (Postgres)
		rows, err := db.Query(`
			SELECT 
				a.actor_id, 
				a.first_name, 
				a.last_name, 
				COALESCE(s.salary, 0) AS salary, 
				COALESCE(d.department_name, 'Unknown') AS department_name
			FROM actor a
			LEFT JOIN salaries s ON a.actor_id = s.actor_id
			LEFT JOIN departments d ON s.department_id = d.department_id
			ORDER BY a.actor_id
		`)
		if err != nil {
			log.Println("Error fetching data from source database:", err)
			http.Error(w, "Error fetching data from source database", http.StatusInternalServerError)
			return
		}
		defer rows.Close()

		// Prepare the data to be inserted/updated in the target database
		var actors []Actor
		for rows.Next() {
			var actor Actor
			err := rows.Scan(&actor.ActorID, &actor.FirstName, &actor.LastName, &actor.Salary, &actor.DepartmentName)
			if err != nil {
				log.Println("Error scanning data:", err)
				http.Error(w, "Error reading data", http.StatusInternalServerError)
				return
			}
			actors = append(actors, actor)
		}

		// Check if no data was found
		if len(actors) == 0 {
			log.Println("No actors found to sync")
		}

		// Insert/Update the data into the target database
		for _, actor := range actors {
			// Check if actor exists in target_db, then decide to update or insert
			var targetActor Actor
			err := targetDB.QueryRow("SELECT actor_id, first_name, last_name FROM actor WHERE actor_id = $1", actor.ActorID).Scan(&targetActor.ActorID, &targetActor.FirstName, &targetActor.LastName)

			if err != nil && err != sql.ErrNoRows {
				log.Println("Error checking target database:", err)
				http.Error(w, "Error checking target database", http.StatusInternalServerError)
				return
			}

			// If the actor exists, update it
			if err == nil {
				// Actor exists in target DB, update it
				_, err = targetDB.Exec(`
					UPDATE actor
					SET first_name = $1, last_name = $2
					WHERE actor_id = $3
				`, actor.FirstName, actor.LastName, actor.ActorID)

				if err != nil {
					log.Println("Error updating actor in target database:", err)
					http.Error(w, "Error updating actor data", http.StatusInternalServerError)
					return
				}

				// Update the salary and department as well
				_, err = targetDB.Exec(`
					UPDATE salaries SET salary = $1 WHERE actor_id = $2
				`, actor.Salary, actor.ActorID)

				if err != nil {
					log.Println("Error updating salary in target database:", err)
					http.Error(w, "Error updating salary data", http.StatusInternalServerError)
					return
				}

				_, err = targetDB.Exec(`
					UPDATE departments SET department_name = $1 WHERE department_id = (SELECT department_id FROM salaries WHERE actor_id = $2)
				`, actor.DepartmentName, actor.ActorID)

				if err != nil {
					log.Println("Error updating department in target database:", err)
					http.Error(w, "Error updating department data", http.StatusInternalServerError)
					return
				}

			} else {
				// Actor does not exist in target DB, insert it
				_, err = targetDB.Exec(`
					INSERT INTO actor (actor_id, first_name, last_name)
					VALUES ($1, $2, $3)
				`, actor.ActorID, actor.FirstName, actor.LastName)

				if err != nil {
					log.Println("Error inserting actor into target database:", err)
					http.Error(w, "Error inserting actor data", http.StatusInternalServerError)
					return
				}

				// Insert salary data
				_, err = targetDB.Exec(`
					INSERT INTO salaries (actor_id, salary)
					VALUES ($1, $2)
				`, actor.ActorID, actor.Salary)

				if err != nil {
					log.Println("Error inserting salary into target database:", err)
					http.Error(w, "Error inserting salary data", http.StatusInternalServerError)
					return
				}

				// Insert department data
				_, err = targetDB.Exec(`
					INSERT INTO departments (department_name)
					VALUES ($1)
				`, actor.DepartmentName)

				if err != nil {
					log.Println("Error inserting department into target database:", err)
					http.Error(w, "Error inserting department data", http.StatusInternalServerError)
					return
				}
			}

			log.Printf("Actor with ID %d synced successfully.\n", actor.ActorID)
		}

		// Return success response
		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(map[string]string{"message": "Data synced successfully"})
		if err != nil {
			log.Println("Error encoding response:", err)
		}
	})

	// Apply CORS middleware to allow cross-origin requests
	handler := cors.Default().Handler(mux)

	// Start the server
	fmt.Println("Server running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", handler))
}
