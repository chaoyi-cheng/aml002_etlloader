CREATE TABLE aml.etl_transactions (
    tran_id VARCHAR(50) NOT NULL,
    entity_id VARCHAR(50) NOT NULL,
    tran_type VARCHAR(50) NOT NULL,
    tran_amount_int INTEGER NOT NULL,
    tran_amount_float FLOAT NOT NULL,
    tran_currency VARCHAR(3) NOT NULL,
	tran_datetime DATETIME NOT NULL,
    tran_crdb BOOL NOT NULL,
	tran_details VARCHAR(255),
    PRIMARY KEY (tran_id),
    INDEX idx_entity_id_tran_type (entity_id, tran_type) VISIBLE,
	INDEX idx_tran_datetime (tran_datetime) VISIBLE,
    FOREIGN KEY (entity_id) REFERENCES etl_entity(entity_id)
)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4
;