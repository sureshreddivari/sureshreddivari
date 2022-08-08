DELIMITER $$
DROP PROCEDURE IF EXISTS proc_slow_migrate$$
CREATE PROCEDURE proc_slow_migrate(
	IN source_table VARCHAR(64),      -- Table data will be copied from.
	IN destination_table VARCHAR(64), -- Table data will be copied to.
	IN batch_size INT,                -- Optional - How many records will be copied per batch. Default = 100,000
	IN sleep_time_sec INT,            -- Optional - How many seconds to wait between batches. Default = 5 seconds
	IN pk_name VARCHAR(100),          -- Optional - Column name of primary key. Default = 'id'
	IN start_id BIGINT,               -- Optional - ID to start copying from. Useful if needed to continue. Default = 1
	IN end_id BIGINT,				  -- Optional - ID to end copying to. Default = MAX(id)
  IN read_uncommitted BOOLEAN,      -- Optional - Lock mode for batches. Default = false
	IN continue_from_last BOOLEAN	  -- Optional - Will attempt to set start_id from last batch. Default = false
)
BEGIN
	SET batch_size = IFNULL(batch_size, 100000); -- 100,000
	SET sleep_time_sec = IFNULL(sleep_time_sec, 5); -- 5 seconds
	SET start_id = IFNULL(start_id, 1);
	SET pk_name = IFNULL(pk_name, 'id');
  SET read_uncommitted = IFNULL(read_uncommitted, false); -- READ COMMITTED

	IF end_id IS NULL THEN
		-- Get highest id. Count may be inaccurate if inconsistent records.
		SET @qry = CONCAT('SELECT MAX(', pk_name, ') INTO @max_id FROM ', source_table);
		PREPARE stmt FROM @qry;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
	ELSE
		SET @max_id = end_id;
	END IF;

	-- Create log table.
	CREATE TABLE IF NOT EXISTS `slow_migrate_log` (
		`id` INT(11) NOT NULL AUTO_INCREMENT,
		`source_table` VARCHAR(64) NOT NULL,
		`destination_table` VARCHAR(64) NOT NULL,
		`started_at` DATETIME NOT NULL,
		`execution_time_sec` DECIMAL(10,3),
		`found_rows` BIGINT(20),
		`row_start` BIGINT(20),
		`row_end` BIGINT(20),
		`percent_complete` DECIMAL(7,4),
		PRIMARY KEY(`id`)
	);

	-- If continue = true and job is unfinished, overwrite the start_id.
	IF continue_from_last = true THEN
		SET @qry = CONCAT(
			'SELECT (max(row_end) + 1) into @continue_id ',
			'FROM `slow_migrate_log` ',
			'WHERE `source_table` = ''', source_table, ''' AND `destination_table` = ''', destination_table, ''''
		);
		PREPARE stmt FROM @qry;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;

		SET start_id = IFNULL(@continue_id, start_id);
	END IF;

	-- Insert statement that copies records from source => destination.
	SET @current_id = start_id;
	SET @qry = CONCAT(
		'INSERT IGNORE INTO `', destination_table, '`',
		'SELECT * FROM `', source_table, '`',
		'WHERE `', pk_name, '` BETWEEN ? AND ?'
	);
	PREPARE stmt FROM @qry;

	WHILE @current_id <= @max_id DO
		SET @last_id = @current_id + batch_size - 1;
    IF @last_id > @max_id THEN
        SET @last_id = @max_id;
    END IF;

		SET @time_start = NOW(3);

		-- Exec copy statement.
    IF read_uncommitted = true THEN
	  	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
    ELSE
      SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
    END IF;

		EXECUTE stmt USING @current_id, @last_id;
		COMMIT;

		SET @rows_in_batch = (SELECT FOUND_ROWS());

		-- Update log table
		INSERT INTO `slow_migrate_log` (
			`source_table`,
			`destination_table`,
			`started_at`,
			`execution_time_sec`,
			`found_rows`,
			`row_start`,
			`row_end`,
			`percent_complete`) VALUES (
			source_table,
			destination_table,
			@time_start,
			(UNIX_TIMESTAMP(NOW(3))*1000 - UNIX_TIMESTAMP(@time_start)*1000) / 1000,
			@rows_in_batch,
			@current_id,
			@last_id,
			(@last_id / @max_id * 100)
		);

		SET @current_id = @last_id + 1;

		IF @rows_in_batch = 0 THEN
			DO SLEEP(0.1);
		ELSE
			DO SLEEP(sleep_time_sec);
		END IF;
	END WHILE;

	DEALLOCATE PREPARE stmt;
END$$
-- call proc_slow_migrate('history', 'history_bigint', 10000, 0.5, 'auto_id', null, NULL, FALSE,1);
-- select * from slow_migrate_log order by id desc limit 10;
DELIMITER ;
