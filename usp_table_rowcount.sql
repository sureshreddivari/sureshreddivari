/*
-- drop table tmp_table_rowcount_test;

-- call usp_table_row_count('history','auto_id', 1000000);
-- call usp_table_row_count_3 ('service_use_tracking','auto_id',null, null, 200);

-- select * from tmp_table_rowcount_service_use_tracking order by id desc limit 5;

-- call usp_table_row_count ('integration_log','id',null, null, 1000000);

*/

DELIMITER $$
DROP PROCEDURE IF EXISTS usp_table_row_count$$

create PROCEDURE usp_table_row_count
	( 
    v_table varchar(64), 
    v_pk_name varchar(64), 
    start_id int,
    end_id int,
    batch_size INT
    )
BEGIN

    declare v_minid bigint ;
    declare v_maxid bigint;
	declare v_total_count bigint ;
    declare v_id bigint;
    declare v_lastid bigint;
   
 	SET @qry = CONCAT('create table if not exists tmp_table_rowcount_', v_table, ' (id int auto_increment primary key, from_id int, to_id int, count int, total_count int)');
    PREPARE stmt FROM @qry;
     -- select @qry;
	 EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
    
    select concat('table : tmp_table_rowcount_', v_table);
    
	-- If start_id is null, find the last id processed and continue from there
	IF start_id is null THEN
		SET @qry = CONCAT('SELECT (max(to_id) + 1),',' max(total_count) into @continue_id,',' @total_count' ,	' FROM tmp_table_rowcount_', v_table );
		-- select @qry;
	
		PREPARE stmt FROM @qry;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
        SET v_minid = IFNULL(@continue_id, 1);
  		       
	else
		set v_minid=start_id;
	end if;
	
	-- If end_id is null, find the max value of table and use it
   IF end_id IS NULL THEN
	-- Get highest id. Count may be inaccurate if inconsistent records.
		SET @qry = CONCAT('SELECT MAX(', v_pk_name, ') INTO @max_id FROM ', v_table);
		PREPARE stmt FROM @qry;
		-- select @qry;
		EXECUTE stmt;
		DEALLOCATE PREPARE stmt;
		set v_maxid=@max_id;
	ELSE
		SET v_maxid = end_id;
	END IF;





	select concat(' minid: ', v_minid, '    maxid: ', v_maxid, '    BatchSize  :', batch_size);
   

	SET v_total_count = IFNULL(@total_count, 0);


 
	set v_id=v_minid;
 
 	-- SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
        
	WHILE v_id <= v_maxid 
    DO 

    set v_lastid= v_id + batch_size - 1;
    
   select v_id, v_lastid;
  
	SET @qry = CONCAT('SELECT count(1) into @count FROM  ', v_table , '  where  ', v_pk_name, '  >= ', v_id, '  and  ',v_pk_name, ' <= ', v_lastid);
    PREPARE stmt FROM @qry;
     -- select @qry;
	 EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
    
    set v_total_count=v_total_count+@count;
   
	SET @qry = CONCAT('insert into tmp_table_rowcount_', v_table , ' (from_id, to_id, count, total_count) values (', v_id, ', ', v_lastid, ',', @count, ',', v_total_count,')') ;
    PREPARE stmt FROM @qry;
     -- select @qry;
	EXECUTE stmt;
	DEALLOCATE PREPARE stmt;
    

        
	

	--  select concat(' count of records betwen ID : ', v_id , ' and ', (v_id + batch_size), ' is : ', @count)  as current_count_status;

      set  v_id = v_lastid + 1;

 end while;
 
 select concat('Total record count :', v_total_count);



end$$

DELIMITER ;


