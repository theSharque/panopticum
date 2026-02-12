CREATE DATABASE IF NOT EXISTS panopticum_dev;

CREATE TABLE IF NOT EXISTS panopticum_dev.users
(
    id UInt32,
    name String,
    email String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY id;

INSERT INTO panopticum_dev.users (id, name, email) VALUES
(1, 'Alice', 'alice@example.com'),
(2, 'Bob', 'bob@example.com'),
(3, 'Carol', 'carol@example.com'),
(4, 'Dave', 'dave@example.com'),
(5, 'Eve', 'eve@example.com');

CREATE TABLE IF NOT EXISTS panopticum_dev.events
(
    event_id UUID DEFAULT generateUUIDv4(),
    user_id UInt32,
    event_type String,
    payload String,
    created_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (user_id, created_at);

INSERT INTO panopticum_dev.events (user_id, event_type, payload) VALUES
(1, 'login', '{"ip": "192.168.1.1"}'),
(1, 'page_view', '{"path": "/dashboard"}'),
(2, 'login', '{"ip": "10.0.0.5"}'),
(2, 'purchase', '{"amount": 99.99, "currency": "USD"}'),
(3, 'signup', '{}'),
(4, 'login', '{"ip": "172.16.0.1"}'),
(4, 'page_view', '{"path": "/settings"}'),
(5, 'logout', '{}');

CREATE TABLE IF NOT EXISTS panopticum_dev.metrics
(
    ts DateTime,
    name String,
    value Float64,
    tags String
)
ENGINE = MergeTree()
ORDER BY (name, ts);

INSERT INTO panopticum_dev.metrics (ts, name, value, tags) VALUES
(now() - 3600, 'cpu_usage', 12.5, 'host=server1'),
(now() - 3600, 'memory_mb', 2048.0, 'host=server1'),
(now() - 1800, 'cpu_usage', 18.2, 'host=server1'),
(now() - 1800, 'memory_mb', 2100.5, 'host=server1'),
(now() - 3600, 'cpu_usage', 8.1, 'host=server2'),
(now() - 3600, 'memory_mb', 1024.0, 'host=server2'),
(now(), 'cpu_usage', 15.0, 'host=server1'),
(now(), 'memory_mb', 2050.0, 'host=server1');
