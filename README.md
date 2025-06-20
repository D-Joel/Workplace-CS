# Workplace-CS
Great — removing the static UMT table and directly using the stage table with dynamic selection is a much cleaner and scalable solution.

Based on your setup, here’s a **complete Python script** that does the following:

---

### ✅ **Script Functions:**

1. **Connect to BigQuery**.
2. **Fetch a batch (e.g., 3) of `PENDING` files** from the stage table.
3. **Mark them as `IN_PROGRESS`**.
4. For each file:

   * Run the BQ query and generate file.
   * Store it to GCS.
   * Push it to SFTP.
   * Update the status (`SUCCESS`, `FAILURE`, etc.).
5. **Parallelize file processing** using `multiprocessing`.

---

### 📦 **Requirements**

Install if not already:

```bash
pip install google-cloud-bigquery google-cloud-storage paramiko
```

---

### 🐍 **Full Python Script**

```python
import os
import time
import paramiko
from multiprocessing import Pool
from google.cloud import bigquery, storage
import pandas as pd
import psycopg2  # Or your preferred DB library

# CONFIG
BATCH_SIZE = 3
STAGE_TABLE = 'project.dataset.stage_table'
STATUS_TABLE = 'project.dataset.status_table'
GCS_BUCKET = 'your-gcs-bucket'
SFTP_HOST = 'your-sftp-host'
SFTP_USER = 'your-user'
SFTP_PASS = 'your-password'
SFTP_PATH = '/upload/path/'


def get_db_connection():
    return psycopg2.connect(
        host='your-db-host',
        database='your-db',
        user='your-user',
        password='your-password'
    )


def fetch_pending_files():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        UPDATE {STAGE_TABLE}
        SET status = 'IN_PROGRESS'
        WHERE id IN (
            SELECT id FROM {STAGE_TABLE}
            WHERE status = 'PENDING'
            ORDER BY RANDOM()
            LIMIT {BATCH_SIZE}
        )
        RETURNING id, file_name, source_query;
    """)
    rows = cursor.fetchall()
    conn.commit()
    conn.close()
    return [{'id': r[0], 'file_name': r[1], 'query': r[2]} for r in rows]


def update_status(file_id, status, message=''):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        INSERT INTO {STATUS_TABLE} (id, status, message, updated_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (id) DO UPDATE
        SET status = EXCLUDED.status,
            message = EXCLUDED.message,
            updated_at = EXCLUDED.updated_at;
    """, (file_id, status, message[:500]))
    conn.commit()
    conn.close()


def upload_to_gcs(local_path, gcs_path):
    client = storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(local_path)
    print(f'Uploaded to GCS: {gcs_path}')


def upload_to_sftp(local_path, remote_filename):
    transport = paramiko.Transport((SFTP_HOST, 22))
    transport.connect(username=SFTP_USER, password=SFTP_PASS)
    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.put(local_path, os.path.join(SFTP_PATH, remote_filename))
    sftp.close()
    transport.close()
    print(f'Uploaded to SFTP: {remote_filename}')


def process_file(meta):
    try:
        bq_client = bigquery.Client()
        file_id = meta['id']
        file_name = meta['file_name']
        query = meta['query']

        print(f"Processing: {file_name}")

        # Run BQ query
        df = bq_client.query(query).to_dataframe()
        local_path = f"/tmp/{file_name}"

        # Save to CSV
        df.to_csv(local_path, index=False)
        print(f"Saved: {local_path}")

        # Upload to GCS
        upload_to_gcs(local_path, f"processed/{file_name}")

        # Upload to SFTP
        upload_to_sftp(local_path, file_name)

        # Update status
        update_status(file_id, 'SUCCESS', 'Processed successfully')

        os.remove(local_path)

    except Exception as e:
        print(f"Error in processing file {meta['file_name']}: {str(e)}")
        update_status(meta['id'], 'FAILURE', str(e))


def main():
    files_to_process = fetch_pending_files()

    if not files_to_process:
        print("No pending files to process.")
        return

    with Pool(processes=BATCH_SIZE) as pool:
        pool.map(process_file, files_to_process)


if __name__ == "__main__":
    main()
```

---

### 🔧 Customize:

* Replace `psycopg2` if you're using BigQuery for metadata (just convert SQL accordingly).
* Tweak the SFTP/GCS logic if you use service accounts or SSH keys.
* Modify `local_path` if not on Linux.

---

Would you like me to generate a **diagram or a shell script** that ties this into your **IICS taskflow** call?
