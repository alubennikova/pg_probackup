pg_probackup detach -B /home/anastasia/postgrespro_workdir/backup_dir --instance new --backup-id QNV3LG --storage=s3 --s3-access-key-id="minioadmin" --s3-secret-access-key="minioadmin" --s3-hostname="192.168.1.70:9000" --s3-bucket="newbucket" --s3-force-path-style

Plan:

1. add s3 params
2. detach existing backup to s3. Send each file separately
==  Вы находитесь здесь ==

3. detach existing backup to s3. Pack files to batches.
4. attach backup from s3 back to backup catalog
5. code cleanup
6. full backup to s3
7. restore full backup from s3
8. incremental backup to s3
9. restore incremental backup from s3