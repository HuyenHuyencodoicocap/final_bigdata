-- Kiểm tra cấu hình replication (để Debezium CDC hoạt động)
SHOW wal_level;             -- mong đợi: logical
SHOW max_wal_senders;
SHOW max_replication_slots;
