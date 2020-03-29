package mayton.db.pg;

public enum PgTypes {
    BPCHAR, TEXT, VARCHAR,
    FLOAT8, REAL,
    INT4, SERIAL, SMALLSERIAL, BIGSERIAL, SMALLINT, BIGINT, INTEGER,
    NUMERIC,  DECIMAL,
    TIMESTAMPTZ,
    POINT,
    JSONB
}
