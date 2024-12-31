package main

import (
	"database/sql"
	"flag"
	"log"
	"log/slog"
	"os"
	"strconv"
	"time"

	"github.com/lib/pq"
	_ "github.com/lib/pq"
)

var flagMode string
var connStr = "user=postgres dbname=postgres sslmode=disable port=9932 host=127.0.0.1 password=postgres"

func main() {

	flag.StringVar(&flagMode, "mode", "consumer", "Mode to run in: consumer or publisher")
	flag.Parse()

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		slog.Error("error opening database connection", "error", err)
		os.Exit(1)
	}

	// Create our queue table that holds the tasks
	// To simplify, the payload is just a string
	slog.Info("creating tasks table if needed")
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS tasks (
		id SERIAL PRIMARY KEY,
		name TEXT NOT NULL,
		payload TEXT,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
		executed_at TIMESTAMP
	)`)
	if err != nil {
		slog.Error("error creating tasks table", "error", err)
		os.Exit(1)
	}

	// creating the trigger
	slog.Info("creating trigger if needed")
	_, err = db.Exec(`CREATE OR REPLACE FUNCTION tasks_after_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
  PERFORM pg_notify('tasks_inserted', NEW.id::text);
  RETURN NULL;
END;
$$
LANGUAGE plpgsql;

DO
$$BEGIN
	CREATE TRIGGER tasks_after_insert_trigger
	AFTER INSERT ON tasks
	FOR EACH ROW EXECUTE PROCEDURE tasks_after_insert_trigger();
EXCEPTION
   WHEN duplicate_object THEN
      NULL;
END;$$;
`)
	if err != nil {
		slog.Error("error creating trigger", "error", err)
		os.Exit(1)
	}

	if flagMode == "consumer" {
		consumer(db)
	} else if flagMode == "publisher" {
		publisher(db)
	} else {
		slog.Error("invalid mode", "mode", flagMode)
		os.Exit(1)
	}
}

func consumer(db *sql.DB) {
	slog.Info("running in consumer mode")

	catchup(db)

	listener := pq.NewListener(connStr, 1*time.Second, time.Minute, nil)
	if err := listener.Listen("tasks_inserted"); err != nil {
		log.Fatalf("Failed to listen on channel 'new_task': %v", err)
	}
	slog.Info("Listening for notifications on channel 'tasks_inserted'...")

	slog.Info("at the same time, doing a catchup every 10 seconds")

	ticker := time.NewTicker(10 * time.Second)
	for {
		select {
		case notification := <-listener.Notify:
			if notification == nil {
				// This can happen if the listener is closed or an error occurs
				continue
			}
			log.Printf("Received NOTIFY on channel '%s': new task id = %s\n",
				notification.Channel, notification.Extra)

			id, err := strconv.Atoi(notification.Extra)
			if err != nil {
				slog.Error("error parsing task id", "error", err)
				continue
			}
			doJob(db, id)
		case <-ticker.C:
			catchup(db)
		}

	}
}

func catchup(db *sql.DB) {
	// Process the pending tasks
	var taskID int
	slog.Info("catching up on pending tasks")

	for {
		err := db.QueryRow(`
		SELECT id
	  FROM tasks
	 WHERE executed_at IS NULL
	   FOR UPDATE SKIP LOCKED
	 LIMIT 1
		`).Scan(&taskID)
		if err != nil {
			if err == sql.ErrNoRows {
				slog.Info("no tasks")
				return
			} else {
				slog.Error("error getting task", "error", err)
				return
			}
		} else {
			slog.Info("got task", "taskID", taskID)
			doJob(db, taskID)
		}
	}
}

func doJob(db *sql.DB, taskID int) {

	var taskName string
	var taskPayload string

	err := db.QueryRow(`SELECT name, payload FROM tasks WHERE id = $1`, taskID).Scan(&taskName, &taskPayload)
	if err != nil {
		slog.Error("error getting task", "error", err)
		return
	}
	slog.Info("executing task", "taskID", taskID, "taskName", taskName, "taskPayload", taskPayload)
	// Execute the task
	_, err = db.Exec(`UPDATE tasks SET executed_at = CURRENT_TIMESTAMP WHERE id = $1`, taskID)
	if err != nil {
		slog.Error("error executing task", "error", err)
	}
}

func publisher(db *sql.DB) {
	slog.Info("running in publisher mode. Publishing a task per second")

	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		var id int
		err := db.QueryRow(`INSERT INTO tasks (name, payload) VALUES ($1, $2) RETURNING id`, "task", "payload").Scan(&id)
		if err != nil {
			slog.Error("error inserting task", "error", err)
		}

		slog.Info("published task", "taskID", id)

		// publishing the task

	}
}

func doTask(db *sql.DB, taskID int) {
	slog.Info("executing task", "taskID", taskID)
	// Execute the task
	_, err := db.Exec(`UPDATE tasks SET executed_at = CURRENT_TIMESTAMP WHERE id = $1`, taskID)
	if err != nil {
		slog.Error("error executing task", "error", err)
	}
}
