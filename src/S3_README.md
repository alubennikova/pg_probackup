pg_probackup detach -B /home/anastasia/postgrespro_workdir/backup_dir --instance new --backup-id QNV3LG --storage=s3 --s3-access-key-id="minioadmin" --s3-secret-access-key="minioadmin" --s3-hostname="192.168.1.70:9000" --s3-bucket="newbucket" --s3-force-path-style

Plan:

- add s3 params
- detach existing backup to s3. Send each file separately
- detach existing backup to s3. Pack files to batches.
- 