package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/go-redis/redis/v8" // Redis client for Go
	_ "github.com/lib/pq"
	"github.com/rs/cors"
)

// Actor struct for JSON response
type Actor struct {
	ActorID        int     `json:"actor_id"`
	FirstName      string  `json:"first_name"`
	LastName       string  `json:"last_name"`
	Salary         float64 `json:"salary"`
	DepartmentName string  `json:"department_name"`
	DepartmentID   int     `json:"department_id"`
}

// Global database connections
var db *sql.DB
var targetDB *sql.DB
var redisClient *redis.Client // Redis client for caching
var ctx = context.Background()

// Cache configuration
const (
	CACHE_DURATION = 10 * time.Minute // How long items remain in cache
	ACTOR_KEY_PREFIX = "actor:"       // Prefix for actor keys in Redis
	ALL_ACTORS_KEY = "all_actors"     // Key for storing all actors list
)

func main() {
	var err error

	// Initialize Source Database
	db, err = sql.Open("postgres", "postgresql://postgres:1234@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		log.Fatal("❌ Error connecting to the source database:", err)
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Initialize Target Database
	targetDB, err = sql.Open("postgres", "postgresql://postgres:1234@localhost:5432/target_db?sslmode=disable")
	if err != nil {
		log.Fatal("❌ Error connecting to the target database:", err)
	}
	targetDB.SetMaxOpenConns(100)
	targetDB.SetMaxIdleConns(25)
	targetDB.SetConnMaxLifetime(5 * time.Minute)

	// Initialize Redis client for caching
	redisClient = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379", // Default Redis address
		Password: "",               // No password
		DB:       0,                // Default DB
	})

	// Test Redis connection
	_, err = redisClient.Ping(ctx).Result()
	if err != nil {
		log.Println("⚠️ Warning: Redis connection failed:", err)
		log.Println("⚠️ Continuing without caching")
		redisClient = nil // Set to nil so we can check if Redis is available
	} else {
		fmt.Println("✅ Connected to Redis cache successfully!")
	}

	// Verify database connections
	if err = db.Ping(); err != nil {
		log.Fatal("❌ Error pinging the source database:", err)
	}
	fmt.Println("✅ Connected to the source database successfully!")

	if err = targetDB.Ping(); err != nil {
		log.Fatal("❌ Error pinging the target database:", err)
	}
	fmt.Println("✅ Connected to the target database successfully!")

	// Ensure target database has necessary tables
	ensureTargetSchema()

	// Create a new router
	mux := http.NewServeMux()
	mux.HandleFunc("/actors", fetchActors)
	mux.HandleFunc("/actor/", fetchSingleActor) // New endpoint for single actor with cache
	mux.HandleFunc("/sync", syncData)
	mux.HandleFunc("/clear-cache", clearCache) // New endpoint to clear cache

	// Apply CORS middleware
	handler := cors.Default().Handler(mux)

	// Start the server
	fmt.Println("🚀 Server running on port 8080...")
	log.Fatal(http.ListenAndServe(":8080", handler))
}

// Ensure target database has the necessary schema
func ensureTargetSchema() {
	// Create departments table if it doesn't exist
	_, err := targetDB.Exec(`
		CREATE TABLE IF NOT EXISTS departments (
			department_id SERIAL PRIMARY KEY,
			department_name VARCHAR(100) NOT NULL
		)
	`)
	if err != nil {
		log.Fatal("❌ Error creating departments table:", err)
	}

	// Create actor table if it doesn't exist
	_, err = targetDB.Exec(`
		CREATE TABLE IF NOT EXISTS actor (
			actor_id SERIAL PRIMARY KEY,
			first_name VARCHAR(100) NOT NULL,
			last_name VARCHAR(100) NOT NULL
		)
	`)
	if err != nil {
		log.Fatal("❌ Error creating actor table:", err)
	}

	// Create salaries table if it doesn't exist
	_, err = targetDB.Exec(`
		CREATE TABLE IF NOT EXISTS salaries (
			salary_id SERIAL PRIMARY KEY,
			actor_id INTEGER REFERENCES actor(actor_id),
			department_id INTEGER REFERENCES departments(department_id),
			salary NUMERIC(10,2) NOT NULL
		)
	`)
	if err != nil {
		log.Fatal("❌ Error creating salaries table:", err)
	}

	log.Println("✅ Target database schema verified/created")
}

// Clear the Redis cache - exposed as an API endpoint
func clearCache(w http.ResponseWriter, r *http.Request) {
	if redisClient == nil {
		w.WriteHeader(http.StatusServiceUnavailable)
		json.NewEncoder(w).Encode(map[string]string{"message": "Cache not available"})
		return
	}

	// Delete the all actors cache
	err := redisClient.Del(ctx, ALL_ACTORS_KEY).Err()
	if err != nil {
		log.Printf("⚠️ Error clearing all actors cache: %v", err)
	}

	// Find all actor keys and delete them
	keys, err := redisClient.Keys(ctx, ACTOR_KEY_PREFIX+"*").Result()
	if err != nil {
		log.Printf("⚠️ Error finding actor keys: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"message": "Error clearing cache"})
		return
	}

	// If we found keys, delete them
	if len(keys) > 0 {
		err = redisClient.Del(ctx, keys...).Err()
		if err != nil {
			log.Printf("⚠️ Error deleting actor keys: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"message": "Error clearing cache"})
			return
		}
	}

	log.Printf("✅ Cache cleared successfully. Removed %d keys", len(keys)+1)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": fmt.Sprintf("Cache cleared successfully. Removed %d keys", len(keys)+1)})
}

// Fetch a single actor with caching
func fetchSingleActor(w http.ResponseWriter, r *http.Request) {
	// Extract actor ID from URL path
	actorIDStr := r.URL.Path[len("/actor/"):]
	actorID, err := strconv.Atoi(actorIDStr)
	if err != nil {
		http.Error(w, "Invalid actor ID", http.StatusBadRequest)
		return
	}

	log.Printf("📌 Fetching actor with ID: %d", actorID)
	
	var actor Actor
	cacheKey := fmt.Sprintf("%s%d", ACTOR_KEY_PREFIX, actorID)
	
	// Try to get data from cache first
	if redisClient != nil {
		cachedData, err := redisClient.Get(ctx, cacheKey).Result()
		if err == nil {
			// Cache hit - Unmarshal and return data
			err = json.Unmarshal([]byte(cachedData), &actor)
			if err == nil {
				log.Printf("✅ Cache hit for actor: %d", actorID)
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Cache", "HIT")
				json.NewEncoder(w).Encode(actor)
				return
			}
			log.Printf("⚠️ Error unmarshaling cached data: %v", err)
		}
	}

	// Cache miss or Redis not available - Query the database
	log.Printf("📌 Cache miss for actor: %d, querying database", actorID)
	row := db.QueryRow(`
		SELECT a.actor_id, a.first_name, a.last_name, 
			   COALESCE(s.salary, 0) AS salary,
			   COALESCE(d.department_id, 0) AS department_id,
			   COALESCE(d.department_name, 'Unknown') AS department_name
		FROM actor a
		LEFT JOIN salaries s ON a.actor_id = s.actor_id
		LEFT JOIN departments d ON s.department_id = d.department_id
		WHERE a.actor_id = $1
	`, actorID)

	err = row.Scan(&actor.ActorID, &actor.FirstName, &actor.LastName, 
				  &actor.Salary, &actor.DepartmentID, &actor.DepartmentName)
	if err == sql.ErrNoRows {
		http.Error(w, "Actor not found", http.StatusNotFound)
		return
	} else if err != nil {
		log.Printf("❌ Error fetching actor: %v", err)
		http.Error(w, "Error fetching actor data", http.StatusInternalServerError)
		return
	}

	// Store in cache if Redis is available
	if redisClient != nil {
		actorJSON, err := json.Marshal(actor)
		if err == nil {
			err = redisClient.Set(ctx, cacheKey, actorJSON, CACHE_DURATION).Err()
			if err != nil {
				log.Printf("⚠️ Error caching actor data: %v", err)
			} else {
				log.Printf("✅ Stored actor %d in cache", actorID)
			}
		}
	}

	// Return the actor data
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	json.NewEncoder(w).Encode(actor)
}

// Fetch actors from the database with JSON streaming and caching
func fetchActors(w http.ResponseWriter, r *http.Request) {
	log.Println("📌 Received request to /actors")

	// Try to get from cache first if Redis is available
	if redisClient != nil {
		cachedData, err := redisClient.Get(ctx, ALL_ACTORS_KEY).Result()
		if err == nil {
			// We found the cached data, return it directly
			log.Println("✅ Returning actors from cache")
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			w.Write([]byte(cachedData))
			return
		}
	}

	// Cache miss or Redis not available - Query the database
	log.Println("📌 Cache miss for all actors, querying database")
	rows, err := db.Query(`
		SELECT a.actor_id, a.first_name, a.last_name, 
		       COALESCE(s.salary, 0) AS salary,
		       COALESCE(d.department_id, 0) AS department_id,
		       COALESCE(d.department_name, 'Unknown') AS department_name
		FROM actor a
		LEFT JOIN salaries s ON a.actor_id = s.actor_id
		LEFT JOIN departments d ON s.department_id = d.department_id
		ORDER BY a.actor_id
	`)
	if err != nil {
		log.Println("❌ Error fetching data:", err)
		http.Error(w, "Error fetching data", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	// If Redis is available, prepare to cache the complete result
	var cacheBuffer []byte
	if redisClient != nil {
		cacheBuffer = make([]byte, 0, 1024*1024) // Pre-allocate 1MB buffer for the cache
		cacheBuffer = append(cacheBuffer, '[')   // Start JSON array
	}

	// Stream JSON response
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Cache", "MISS")
	w.Write([]byte("["))
	first := true

	for rows.Next() {
		var actor Actor
		if err := rows.Scan(&actor.ActorID, &actor.FirstName, &actor.LastName, 
						   &actor.Salary, &actor.DepartmentID, &actor.DepartmentName); err != nil {
			log.Println("❌ Error scanning data:", err)
			http.Error(w, "Error reading data", http.StatusInternalServerError)
			return
		}

		// Convert actor to JSON
		actorJSON, err := json.Marshal(actor)
		if err != nil {
			log.Println("❌ Error encoding JSON:", err)
			http.Error(w, "Error encoding data", http.StatusInternalServerError)
			return
		}

		// Add comma if not first element
		if !first {
			w.Write([]byte(","))
			if redisClient != nil {
				cacheBuffer = append(cacheBuffer, ',')
			}
		}
		first = false

		// Write to response
		w.Write(actorJSON)

		// Add to cache buffer if Redis is available
		if redisClient != nil {
			cacheBuffer = append(cacheBuffer, actorJSON...)
			
			// Also cache individual actor
			actorKey := fmt.Sprintf("%s%d", ACTOR_KEY_PREFIX, actor.ActorID)
			err = redisClient.Set(ctx, actorKey, actorJSON, CACHE_DURATION).Err()
			if err != nil {
				log.Printf("⚠️ Error caching actor %d: %v", actor.ActorID, err)
			}
		}
	}

	// Close JSON array
	w.Write([]byte("]"))

	// Store complete result in cache if Redis is available
	if redisClient != nil {
		cacheBuffer = append(cacheBuffer, ']')
		err = redisClient.Set(ctx, ALL_ACTORS_KEY, cacheBuffer, CACHE_DURATION).Err()
		if err != nil {
			log.Printf("⚠️ Error caching all actors: %v", err)
		} else {
			log.Println("✅ Cached all actors successfully")
		}
	}
}

// Sync data between source and target database
func syncData(w http.ResponseWriter, r *http.Request) {
	log.Println("📌 Starting full data sync...")

	// First, sync departments
	if err := syncDepartments(); err != nil {
		log.Println("❌ Error syncing departments:", err)
		http.Error(w, "Error syncing departments", http.StatusInternalServerError)
		return
	}
	log.Println("✅ Departments synced successfully")

	// Then, sync actors and salaries
	rows, err := db.Query(`
		SELECT a.actor_id, a.first_name, a.last_name, 
		       COALESCE(s.salary, 0) AS salary,
		       COALESCE(d.department_id, 0) AS department_id,
		       COALESCE(d.department_name, 'Unknown') AS department_name
		FROM actor a
		LEFT JOIN salaries s ON a.actor_id = s.actor_id
		LEFT JOIN departments d ON s.department_id = d.department_id
		ORDER BY a.actor_id
	`)
	if err != nil {
		log.Println("❌ Error fetching data from source:", err)
		http.Error(w, "Error fetching data", http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	var wg sync.WaitGroup
	actorChannel := make(chan Actor, 10)

	numWorkers := 5
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go worker(&wg, actorChannel)
	}

	count := 0
	for rows.Next() {
		var actor Actor
		if err := rows.Scan(&actor.ActorID, &actor.FirstName, &actor.LastName, 
						   &actor.Salary, &actor.DepartmentID, &actor.DepartmentName); err != nil {
			log.Println("❌ Error scanning data:", err)
			http.Error(w, "Error reading data", http.StatusInternalServerError)
			return
		}
		actorChannel <- actor
		count++
	}
	close(actorChannel)
	wg.Wait()

	// Clear the cache after sync to ensure data consistency
	if redisClient != nil {
		log.Println("📌 Clearing cache after sync...")
		
		// Delete main keys
		redisClient.Del(ctx, ALL_ACTORS_KEY)
		
		// Find and delete all actor keys
		keys, err := redisClient.Keys(ctx, ACTOR_KEY_PREFIX+"*").Result()
		if err == nil && len(keys) > 0 {
			redisClient.Del(ctx, keys...)
		}
		
		log.Println("✅ Cache cleared after sync")
	}

	log.Printf("✅ Data synced successfully. Processed %d records.\n", count)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": fmt.Sprintf("Data synced successfully. Processed %d records.", count)})
}

// Sync departments from source to target
func syncDepartments() error {
	rows, err := db.Query(`SELECT department_id, department_name FROM departments`)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return err
		}

		_, err = targetDB.Exec(`
			INSERT INTO departments (department_id, department_name) 
			VALUES ($1, $2) 
			ON CONFLICT (department_id) DO UPDATE 
			SET department_name = EXCLUDED.department_name
		`, id, name)
		if err != nil {
			return err
		}
	}
	return nil
}

// Worker function to update target database
func worker(wg *sync.WaitGroup, actorChannel chan Actor) {
	defer wg.Done()

	for actor := range actorChannel {
		tx, err := targetDB.Begin()
		if err != nil {
			log.Printf("❌ Error starting transaction for actor %d: %v\n", actor.ActorID, err)
			continue
		}

		// Insert or update actor
		_, err = tx.Exec(`
			INSERT INTO actor (actor_id, first_name, last_name) 
			VALUES ($1, $2, $3) 
			ON CONFLICT (actor_id) DO UPDATE 
			SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name
		`, actor.ActorID, actor.FirstName, actor.LastName)
		if err != nil {
			log.Printf("❌ Error updating actor %d: %v\n", actor.ActorID, err)
			tx.Rollback()
			continue
		}

		// Skip salary update if no department exists
		if actor.DepartmentID > 0 {
			// Check if salary record exists
			var salaryID int
			err = tx.QueryRow(`SELECT salary_id FROM salaries WHERE actor_id = $1`, actor.ActorID).Scan(&salaryID)
			
			if err == sql.ErrNoRows {
				// Insert new salary record
				_, err = tx.Exec(`
					INSERT INTO salaries (actor_id, department_id, salary) 
					VALUES ($1, $2, $3)
				`, actor.ActorID, actor.DepartmentID, actor.Salary)
				if err != nil {
					log.Printf("❌ Error inserting salary for actor %d: %v\n", actor.ActorID, err)
					tx.Rollback()
					continue
				}
			} else if err == nil {
				// Update existing salary record
				_, err = tx.Exec(`
					UPDATE salaries 
					SET department_id = $1, salary = $2 
					WHERE actor_id = $3
				`, actor.DepartmentID, actor.Salary, actor.ActorID)
				if err != nil {
					log.Printf("❌ Error updating salary for actor %d: %v\n", actor.ActorID, err)
					tx.Rollback()
					continue
				}
			} else {
				// Unexpected error
				log.Printf("❌ Error checking salary for actor %d: %v\n", actor.ActorID, err)
				tx.Rollback()
				continue
			}
		}

		if err := tx.Commit(); err != nil {
			log.Printf("❌ Error committing transaction for actor %d: %v\n", actor.ActorID, err)
		}
	}
}
