1. **Redpanda version**

   **Answer:** 
   ~~~
    Version:     v24.2.18
    Git ref:     f9a22d4430
    Build date:  2025-02-14T12:52:55Z
    OS/Arch:     linux/amd64
    Go version:  go1.23.1

    Redpanda Cluster
    node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9
   ~~~

   Explanation:
    - Connect to the shell of <em>redpanda-1</em> container:
        ~~~
        docker compose exec redpanda-1 sh
        ~~~
        and execute the following command:
        ~~~
        rpk --version
        ~~~
2. **Creating a topic** 
   
   **Answer:**
   ~~~
    TOPIC        STATUS
    green-trips  OK
   ~~~

   Explanation:
    - Topic is created using the following command:
        ~~~
        rpk topic create green-trips
        ~~~
        in the shell of redpanda-1 container, to which we connect with:
        ~~~
        docker compose exec redpanda-1 sh
        ~~~
3. **Connecting to the Kafka server** 

   **Answer:**
   True 

   Explanation:
    - Answer is the return value of the <em>bootstrap_connected</em> method.

4. **Sending the Trip Data**
   
   **Answer:**
   1791.1940927505493 (seconds)

5. **Build a Sessionization Window**
   
    **Answer:**
   PULocationID with largest streak: 74;
   DOLocationID with largest streak: 138; 

   Explanation
    - Aggregate by <em>count(*)</em> using session window with grouping by target location id(pickup or dropoff).
        To get the final answer, do the queries on tables with aggregation results:
        ~~~
        SELECT *
        FROM processed_events_aggregated_by_pickup
        ORDER BY trips_n DESC
        LIMIT 1;
        ---
        SELECT *
        FROM processed_events_aggregated_by_dropoff
        ORDER BY trips_n DESC
        LIMIT 1;
        ~~~