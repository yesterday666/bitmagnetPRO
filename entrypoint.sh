#!/bin/sh
# Multi-instance bloom filter isolation via database-level key rename.
# Maps blocked_torrents -> blocked_torrents_<HOSTNAME> for each instance.

HOSTNAME=$(hostname)
BLOCK_KEY="blocked_torrents"
INSTANCE_KEY="blocked_torrents_$HOSTNAME"

echo "[$(date '+%H:%M:%S')] Host: $HOSTNAME | Bloom key: $INSTANCE_KEY"

# Try to rename the bloom filter key for this instance if it doesn't exist yet
if [ -n "$POSTGRES_HOST" ]; then
    PGPASSWORD="$POSTGRES_PASSWORD" psql -h "$POSTGRES_HOST" -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "
        DO \$\$
        BEGIN
            -- If shared key exists and instance key doesn't, rename (first-run migration)
            IF EXISTS (SELECT 1 FROM bloom_filters WHERE key = '$BLOCK_KEY')
               AND NOT EXISTS (SELECT 1 FROM bloom_filters WHERE key = '$INSTANCE_KEY') THEN
                UPDATE bloom_filters SET key = '$INSTANCE_KEY' WHERE key = '$BLOCK_KEY';
                RAISE NOTICE 'Migrated bloom filter: % -> %', '$BLOCK_KEY', '$INSTANCE_KEY';
            END IF;
        END;
        \$\$;
    " 2>/dev/null || true
fi

exec bitmagnet "$@"
