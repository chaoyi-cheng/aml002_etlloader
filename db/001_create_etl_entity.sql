CREATE TABLE aml.etl_entity (
    entity_id VARCHAR(50) NOT NULL,
    entity_type VARCHAR(50) NOT NULL,
    entity_name VARCHAR(1000) NOT NULL,
    PRIMARY KEY (entity_id)
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
;
