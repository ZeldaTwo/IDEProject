CREATE TABLE IF NOT EXISTS alerts (
    id          BIGSERIAL PRIMARY KEY,
    ts          BIGINT           NOT NULL,
    device_a    VARCHAR(64)      NOT NULL,
    device_b    VARCHAR(64)      NOT NULL,
    faction_a   VARCHAR(32)      NOT NULL,
    faction_b   VARCHAR(32)      NOT NULL,
    latitude    DOUBLE PRECISION NOT NULL,
    longitude   DOUBLE PRECISION NOT NULL,
    distance_m  DOUBLE PRECISION NOT NULL,
    created_at  TIMESTAMP        NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS combat_results (
    id             BIGSERIAL PRIMARY KEY,
    ts             BIGINT           NOT NULL,
    device_a       VARCHAR(64)      NOT NULL,
    device_b       VARCHAR(64)      NOT NULL,
    winner_faction VARCHAR(32)      NOT NULL,
    loser_faction  VARCHAR(32)      NOT NULL,
    latitude       DOUBLE PRECISION NOT NULL,
    longitude      DOUBLE PRECISION NOT NULL,
    created_at     TIMESTAMP        NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_alerts_ts  ON alerts (ts);
CREATE INDEX IF NOT EXISTS idx_combat_ts  ON combat_results (ts);
