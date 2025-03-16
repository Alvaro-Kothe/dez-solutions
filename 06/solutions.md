# Solutions

1. `v24.2.18`

    ```console
    $ docker compose exec redpanda-1 rpk version
    Version:     v24.2.18
    Git ref:     f9a22d4430
    Build date:  2025-02-14T12:52:55Z
    OS/Arch:     linux/amd64
    Go version:  go1.23.1

    Redpanda Cluster
    node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9

    ```

2. ```
   TOPIC        STATUS
   green-trips  OK
   ```

    ```console
    $ docker compose exec redpanda-1 rpk topic create green-trips
    TOPIC        STATUS
    green-trips  OK
    ```

3. `True`

    ```console
    $ python q3.py
    True
    ```

4. 79.71

    ```console
    $ python q4.py
    took 79.71121907234192 seconds
    ```

5. 75,74

    ```sql
    select * from taxi_events order by pulocationid, dolocationid, session_start;
    ```
