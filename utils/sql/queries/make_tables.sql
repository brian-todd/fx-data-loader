CREATE TABLE raw_ticks
(
    ts TEXT NOT NULL,
    pair TEXT NOT NULL,
    ask REAL NOT NULL,
    bid REAL NOT NULL,
    ask_volume REAL NOT NULL,
    bid_volume REAL NOT NULL,
    PRIMARY KEY (ts, pair)
);

CREATE TABLE minutes
(
    ts TEXT NOT NULL,
    pair TEXT NOT NULL,
    ask REAL NOT NULL,
    bid REAL NOT NULL,
    ask_volume REAL NOT NULL,
    bid_volume REAL NOT NULL,
    PRIMARY KEY (ts, pair)
);

CREATE TABLE hourly
(
    ts TEXT NOT NULL,
    pair TEXT NOT NULL,
    ask REAL NOT NULL,
    bid REAL NOT NULL,
    ask_volume REAL NOT NULL,
    bid_volume REAL NOT NULL,
    PRIMARY KEY (ts, pair)
);

CREATE TABLE daily
(
    ts TEXT NOT NULL,
    pair TEXT NOT NULL,
    ask REAL NOT NULL,
    bid REAL NOT NULL,
    ask_volume REAL NOT NULL,
    bid_volume REAL NOT NULL,
    PRIMARY KEY (ts, pair)
);
