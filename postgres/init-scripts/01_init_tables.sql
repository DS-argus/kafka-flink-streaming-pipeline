-- events 테이블
CREATE TABLE IF NOT EXISTS events (
  display_id     BIGINT,
  uuid           TEXT,
  document_id    BIGINT,
  timestamp    BIGINT,
  geo_location   TEXT,
  platform_id INT,
  event_time TIMESTAMP(3),
  act_prod_time TIMESTAMP(6),
  act_load_time TIMESTAMP(6),
  PRIMARY KEY (uuid, display_id)
);

-- page_views 테이블
CREATE TABLE IF NOT EXISTS page_views (
  uuid           TEXT,
  document_id    BIGINT,
  timestamp    BIGINT,
  geo_location   TEXT,
  traffic_source INT,
  platform_id INT,
  event_time TIMESTAMP(3),
  act_prod_time TIMESTAMP(6),
  act_load_time TIMESTAMP(6),
  PRIMARY KEY (uuid, document_id)
);

-- feature batch update 테이블
CREATE TABLE IF NOT EXISTS features_20m (
  uuid        TEXT    NOT NULL,
  window_end  TIMESTAMP(3) NOT NULL,
  click20m    BIGINT  NOT NULL,
  view20m     BIGINT  NOT NULL,
  PRIMARY KEY (uuid, window_end)
);