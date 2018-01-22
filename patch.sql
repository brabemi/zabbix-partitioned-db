-- history tables chunk size: 100 000 s = 1.15740741 days
--     100000
-- 1516614109
-- 1516600000
-- 1516700000

-- history
-- DROP TABLE history;

CREATE TABLE history (
        itemid                   bigint                                    NOT NULL,
        clock                    integer         DEFAULT '0'               NOT NULL,
        value                    numeric(16,4)   DEFAULT '0.0000'          NOT NULL,
        ns                       integer         DEFAULT '0'               NOT NULL
) PARTITION BY RANGE (clock);
-- CREATE INDEX history_1 ON history (itemid,clock); -- ERROR:  cannot create index on partitioned table "history"

CREATE TABLE history_part_lower PARTITION OF history
    FOR VALUES FROM (MINVALUE) TO (1516600000);
CREATE INDEX history_part_lower_1 ON history_part_lower (itemid,clock);

CREATE TABLE history_part_15166 PARTITION OF history
    FOR VALUES FROM (1516600000) TO (1516700000);
CREATE INDEX history_part_15166_1 ON history_part_15166 (itemid,clock);

CREATE TABLE history_part_upper PARTITION OF history
    FOR VALUES FROM (1516700000) TO (MAXVALUE);
CREATE INDEX history_part_upper_1 ON history_part_upper (itemid,clock);

-- history_uint
-- DROP TABLE history_uint;

CREATE TABLE history_uint (
        itemid                   bigint                                    NOT NULL,
        clock                    integer         DEFAULT '0'               NOT NULL,
        value                    numeric(20)     DEFAULT '0'               NOT NULL,
        ns                       integer         DEFAULT '0'               NOT NULL
) PARTITION BY RANGE (clock);

CREATE TABLE trends_uint_part_lower PARTITION OF trends_uint
    FOR VALUES FROM (MINVALUE) TO (1516600000);
CREATE INDEX trends_uint_part_lower_1 ON trends_uint_part_lower (itemid,clock);

CREATE TABLE trends_uint_part_15166 PARTITION OF trends_uint
    FOR VALUES FROM (1516600000) TO (1516700000);
CREATE INDEX history_uint_part_15166_1 ON history_uint_part_15166 (itemid,clock);

CREATE TABLE history_uint_part_upper PARTITION OF history_uint
    FOR VALUES FROM (1516700000) TO (MAXVALUE);
CREATE INDEX history_uint_part_upper_1 ON history_uint_part_upper (itemid,clock);


-- trends tables chunk size: 1 000 000 s = 11.5740741 days
--    1000000
-- 1516614109
-- 1516000000
-- 1517000000

-- trends
-- DROP TABLE trends;

CREATE TABLE trends (
        itemid                   bigint                                    NOT NULL,
        clock                    integer         DEFAULT '0'               NOT NULL,
        num                      integer         DEFAULT '0'               NOT NULL,
        value_min                numeric(16,4)   DEFAULT '0.0000'          NOT NULL,
        value_avg                numeric(16,4)   DEFAULT '0.0000'          NOT NULL,
        value_max                numeric(16,4)   DEFAULT '0.0000'          NOT NULL
) PARTITION BY RANGE (clock);
-- PRIMARY KEY (itemid,clock) -- primary key constraints are not supported on partitioned tables

CREATE TABLE trends_part_lower PARTITION OF trends
    FOR VALUES FROM (MINVALUE) TO (1516000000);
CREATE UNIQUE INDEX trends_part_lower_1 ON trends_part_lower (itemid,clock);

CREATE TABLE trends_part_15166 PARTITION OF trends
    FOR VALUES FROM (1516000000) TO (1517000000);
CREATE UNIQUE INDEX trends_part_15166_1 ON trends_part_15166 (itemid,clock);

CREATE TABLE trends_part_upper PARTITION OF trends
    FOR VALUES FROM (1517000000) TO (MAXVALUE);
CREATE UNIQUE INDEX trends_part_upper_1 ON trends_part_upper (itemid,clock);

-- trends_uint
-- DROP TABLE trends_uint;

CREATE TABLE trends_uint (
        itemid                   bigint                                    NOT NULL,
        clock                    integer         DEFAULT '0'               NOT NULL,
        num                      integer         DEFAULT '0'               NOT NULL,
        value_min                numeric(20)     DEFAULT '0'               NOT NULL,
        value_avg                numeric(20)     DEFAULT '0'               NOT NULL,
        value_max                numeric(20)     DEFAULT '0'               NOT NULL
) PARTITION BY RANGE (clock);

CREATE TABLE trends_uint_part_lower PARTITION OF trends_uint
    FOR VALUES FROM (MINVALUE) TO (1516000000);
CREATE UNIQUE INDEX trends_uint_part_lower_1 ON trends_uint_part_lower (itemid,clock);

CREATE TABLE trends_uint_part_15166 PARTITION OF trends_uint
    FOR VALUES FROM (1516000000) TO (1517000000);
CREATE UNIQUE INDEX trends_uint_part_15166_1 ON trends_uint_part_15166 (itemid,clock);

CREATE TABLE trends_uint_part_upper PARTITION OF trends_uint
    FOR VALUES FROM (1517000000) TO (MAXVALUE);
CREATE UNIQUE INDEX trends_uint_part_upper_1 ON trends_uint_part_upper (itemid,clock);
