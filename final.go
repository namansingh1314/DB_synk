package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic" // ✅ Required for atomic operations
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
	CACHE_DURATION   = 10 * time.Minute // How long items remain in cache
	ACTOR_KEY_PREFIX = "actor:"         // Prefix for actor keys in Redis
	ALL_ACTORS_KEY   = "all_actors"     // Key for storing all actors list
	BATCH_SIZE       = 1000             // Number of records to process in a single batch
	SCAN_COUNT       = 100              // Number of keys to scan in each Redis SCAN operation
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

	// Ensure target database has necessary tables and indexes
	ensureTargetSchema()
	
	// Ensure source database has necessary indexes for performance
	ensureSourceIndexes()

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

// Ensure source database has necessary indexes for performance
func ensureSourceIndexes() {
	// Check for and create necessary indexes on the source database
	indexQueries := []string{
		`CREATE INDEX IF NOT EXISTS idx_actor_id ON actor(actor_id)`,
		`CREATE INDEX IF NOT EXISTS idx_salaries_actor_id ON salaries(actor_id)`,
		`CREATE INDEX IF NOT EXISTS idx_salaries_department_id ON salaries(department_id)`,
		`CREATE INDEX IF NOT EXISTS idx_departments_id ON departments(department_id)`,
	}

	for _, query := range indexQueries {
		_, err := db.Exec(query)
		if err != nil {
			log.Printf("⚠️ Warning: Error creating source index: %v", err)
		}
	}
	
	log.Println("✅ Source database indexes verified/created")
}

// Ensure target database has the necessary schema and indexes
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
	
	// Add indexes for performance
	indexQueries := []string{
		`CREATE INDEX IF NOT EXISTS idx_target_salaries_actor_id ON salaries(actor_id)`,
		`CREATE INDEX IF NOT EXISTS idx_target_salaries_department_id ON salaries(department_id)`,
		`CREATE INDEX IF NOT EXISTS idx_target_actor_id ON actor(actor_id)`,
		`CREATE INDEX IF NOT EXISTS idx_target_departments_id ON departments(department_id)`,
	}

	for _, query := range indexQueries {
		_, err := targetDB.Exec(query)
		if err != nil {
			log.Printf("⚠️ Warning: Error creating target index: %v", err)
		}
	}

	log.Println("✅ Target database schema and indexes verified/created")
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

	// Use SCAN instead of KEYS to handle millions of keys safely
	// This prevents Redis from blocking during the operation
	var cursor uint64
	var totalDeleted int32
	
	// Process actor keys in parallel using multiple goroutines
	var wg sync.WaitGroup
	workerCount := runtime.NumCPU() // Automatically adjust to available CPU cores
	keysChan := make(chan []string, workerCount)
	doneChan := make(chan struct{})
	errorsChan := make(chan error, workerCount)
	resultsChan := make(chan int, workerCount)
	
	// Start worker goroutines
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			for keys := range keysChan {
				if len(keys) > 0 {
					// Delete keys in batches
					err := redisClient.Del(ctx, keys...).Err()
					if err != nil {
						errorsChan <- err
						return
					}
					resultsChan <- len(keys)
				}
			}
		}()
	}
	
	// Collector goroutine
	go func() {
		wg.Wait()
		close(doneChan)
	}()
	
	// Use SCAN to iterate through keys matching the pattern
	for {
		var keys []string
		var err error
		
		// SCAN operation - safe even with millions of keys
		keys, cursor, err = redisClient.Scan(ctx, cursor, ACTOR_KEY_PREFIX+"*", SCAN_COUNT).Result()
		if err != nil {
			log.Printf("⚠️ Error scanning Redis keys: %v", err)
			w.WriteHeader(http.StatusInternalServerError)
			json.NewEncoder(w).Encode(map[string]string{"message": "Error clearing cache"})
			return
		}
		
		if len(keys) > 0 {
			// Send the batch of keys to a worker
			keysChan <- keys
		}
		
		// Break when we've scanned all keys
		if cursor == 0 {
			break
		}
	}
	
	// Close the keys channel to signal workers we're done
	close(keysChan)
	
	// Wait for all workers to finish or check for errors
	select {
	case err := <-errorsChan:
		log.Printf("⚠️ Error deleting Redis keys: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]string{"message": "Error clearing cache"})
		return
	case <-doneChan:
		// All workers completed successfully
	}
	
	// Count total deleted keys
	close(resultsChan)
	for count := range resultsChan {
		totalDeleted += int32(count)
	}

	log.Printf("✅ Cache cleared successfully. Removed %d keys", totalDeleted+1) // +1 for ALL_ACTORS_KEY
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": fmt.Sprintf("Cache cleared successfully. Removed %d keys", totalDeleted+1)})
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

	// Then, sync actors and salaries using batch processing
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

	// Process actors in batches with dynamic parallelism
	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU() // Use all available CPU cores
	batchChannel := make(chan []Actor, numWorkers*2) // Buffer size based on CPU count
	
	log.Printf("🚀 Using %d worker goroutines based on available CPU cores", numWorkers)

	// Start worker goroutines to process batches
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go batchWorker(&wg, batchChannel)
	}

	// Read actors from database and create batches
	count := 0
	batch := make([]Actor, 0, BATCH_SIZE)
	
	for rows.Next() {
		var actor Actor
		if err := rows.Scan(&actor.ActorID, &actor.FirstName, &actor.LastName, 
						   &actor.Salary, &actor.DepartmentID, &actor.DepartmentName); err != nil {
			log.Println("❌ Error scanning data:", err)
			http.Error(w, "Error reading data", http.StatusInternalServerError)
			return
		}
		
		batch = append(batch, actor)
		count++
		
		// When batch is full, send it to a worker
		if len(batch) >= BATCH_SIZE {
			batchCopy := make([]Actor, len(batch))
			copy(batchCopy, batch)
			batchChannel <- batchCopy
			batch = make([]Actor, 0, BATCH_SIZE)
		}
	}
	
	// Send any remaining actors in the last batch
	if len(batch) > 0 {
		batchChannel <- batch
	}
	
	// Close channel and wait for workers to finish
	close(batchChannel)
	wg.Wait()

	// Clear the cache after sync using SCAN to safely handle large numbers of keys
	if redisClient != nil {
		log.Println("📌 Clearing cache after sync...")
		
		// Delete main keys
		redisClient.Del(ctx, ALL_ACTORS_KEY)
		
		// Use parallel SCAN pattern to clear actor keys
		clearActorKeysWithScan()
		
		log.Println("✅ Cache cleared after sync")
	}

	log.Printf("✅ Data synced successfully. Processed %d records.\n", count)
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"message": fmt.Sprintf("Data synced successfully. Processed %d records.", count)})
}

// Helper function to clear actor keys safely using SCAN
func clearActorKeysWithScan() {
	if redisClient == nil {
		return
	}
	
	var cursor uint64
	var totalDeleted int32
	
	// Use multiple workers for parallel deletion
	var wg sync.WaitGroup
	workerCount := runtime.NumCPU()
	keysChan := make(chan []string, workerCount)
	
	// Start worker goroutines for parallel deletion
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			for keys := range keysChan {
				if len(keys) > 0 {
					err := redisClient.Del(ctx, keys...).Err()
					if err != nil {
						log.Printf("⚠️ Error deleting keys: %v", err)
					} else {
						atomic.AddInt32(&totalDeleted, int32(len(keys)))
					}
				}
			}
		}()
	}
	
	// Use SCAN to iterate through keys matching the pattern
	for {
		var keys []string
		var err error
		
		keys, cursor, err = redisClient.Scan(ctx, cursor, ACTOR_KEY_PREFIX+"*", SCAN_COUNT).Result()
		if err != nil {
			log.Printf("⚠️ Error scanning Redis keys: %v", err)
			break
		}
		
		if len(keys) > 0 {
			keysChan <- keys
		}
		
		if cursor == 0 {
			break
		}
	}
	
	// Close channel and wait for workers to finish
	close(keysChan)
	wg.Wait()
	
	log.Printf("✅ Cleared %d actor keys from cache", totalDeleted)
}

// Sync departments from source to target using batch processing
func syncDepartments() error {
	rows, err := db.Query(`SELECT department_id, department_name FROM departments`)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Process departments in batches with dynamic parallelism
	var wg sync.WaitGroup
	numWorkers := runtime.NumCPU() // Use available CPU cores
	batchChannel := make(chan []struct {
		ID   int
		Name string
	}, numWorkers)
	
	// Error channel for worker errors
	errorChan := make(chan error, numWorkers)
	
	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			
			for batch := range batchChannel {
				if err := bulkInsertDepartments(batch); err != nil {
					errorChan <- err
					return
				}
			}
		}()
	}

	// Collector for departments
	batch := make([]struct {
		ID   int
		Name string
	}, 0, BATCH_SIZE)
	
	count := 0
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			close(batchChannel) // Signal workers to stop
			return err
		}
		
		batch = append(batch, struct {
			ID   int
			Name string
		}{id, name})
		count++
		
		// When batch is full, send to workers
		if len(batch) >= BATCH_SIZE {
			batchCopy := make([]struct {
				ID   int
				Name string
			}, len(batch))
			copy(batchCopy, batch)
			
			// Non-blocking send with error checking
			select {
			case err := <-errorChan:
				close(batchChannel) // Signal workers to stop
				return err
			case batchChannel <- batchCopy:
				// Batch sent successfully
			}
			
			batch = make([]struct {
				ID   int
				Name string
			}, 0, BATCH_SIZE)
		}
	}
	
	// Process any remaining departments
	if len(batch) > 0 {
		select {
		case err := <-errorChan:
			close(batchChannel)
			return err
		case batchChannel <- batch:
			// Batch sent successfully
		}
	}
	
	// Close channel and wait for workers
	close(batchChannel)
	
	// Check for errors before waiting
	select {
	case err := <-errorChan:
		return err
	default:
		// No errors yet
	}
	
	wg.Wait()
	
	// Final error check
	select {
	case err := <-errorChan:
		return err
	default:
		// All good
	}
	
	log.Printf("✅ Synced %d departments using %d workers", count, numWorkers)
	return nil
}

// Helper function to bulk insert departments
func bulkInsertDepartments(departments []struct {
	ID   int
	Name string
}) error {
	tx, err := targetDB.Begin()
	if err != nil {
		return err
	}
	
	// Prepare statement for bulk insert
	stmt, err := tx.Prepare(`
		INSERT INTO departments (department_id, department_name) 
		VALUES ($1, $2) 
		ON CONFLICT (department_id) DO UPDATE 
		SET department_name = EXCLUDED.department_name
	`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer stmt.Close()
	
	// Execute batch inserts within transaction
	for _, dept := range departments {
		_, err = stmt.Exec(dept.ID, dept.Name)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	
	return tx.Commit()
}

// Worker function to process batches of actors
func batchWorker(wg *sync.WaitGroup, batchChannel chan []Actor) {
	defer wg.Done()
	
	for batch := range batchChannel {
		if len(batch) == 0 {
			continue
		}
		
		// Start transaction for the entire batch
		tx, err := targetDB.Begin()
		if err != nil {
			log.Printf("❌ Error starting transaction for batch: %v\n", err)
			continue
		}
		
		// Prepare statements for batch operations
		actorStmt, err := tx.Prepare(`
			INSERT INTO actor (actor_id, first_name, last_name) 
			VALUES ($1, $2, $3) 
			ON CONFLICT (actor_id) DO UPDATE 
			SET first_name = EXCLUDED.first_name, last_name = EXCLUDED.last_name
		`)
		if err != nil {
			log.Printf("❌ Error preparing actor statement: %v\n", err)
			tx.Rollback()
			continue
		}
		defer actorStmt.Close()
		
		// IMPROVED: Use UPSERT instead of separate SELECT + INSERT/UPDATE
		// This eliminates the extra DB round-trips for each actor
		salaryUpsertStmt, err := tx.Prepare(`
			INSERT INTO salaries (actor_id, department_id, salary) 
			VALUES ($1, $2, $3)
			ON CONFLICT (actor_id) DO UPDATE 
			SET department_id = EXCLUDED.department_id, salary = EXCLUDED.salary
		`)
		if err != nil {
			log.Printf("❌ Error preparing salary upsert statement: %v\n", err)
			tx.Rollback()
			continue
		}
		defer salaryUpsertStmt.Close()
		
		// Process all actors in the batch
		batchSuccess := true
		
		for _, actor := range batch {
			// Insert or update actor
			_, err = actorStmt.Exec(actor.ActorID, actor.FirstName, actor.LastName)
			if err != nil {
				log.Printf("❌ Error updating actor %d: %v\n", actor.ActorID, err)
				batchSuccess = false
				break
			}
			
			// Skip salary update if no department exists
			if actor.DepartmentID > 0 {
				// IMPROVED: Use single UPSERT statement instead of SELECT + INSERT/UPDATE
				_, err = salaryUpsertStmt.Exec(actor.ActorID, actor.DepartmentID, actor.Salary)
				if err != nil {
					log.Printf("❌ Error upserting salary for actor %d: %v\n", actor.ActorID, err)
					batchSuccess = false
					break
				}
			}
		}
		
		// Commit or rollback the transaction based on success
		if batchSuccess {
			if err := tx.Commit(); err != nil {
				log.Printf("❌ Error committing batch transaction: %v\n", err)
			} else {
				log.Printf("✅ Successfully processed batch of %d actors\n", len(batch))
			}
		} else {
			tx.Rollback()
			log.Printf("❌Rolled back batch transaction due to errors\n") 
			} 
		} 
	}
