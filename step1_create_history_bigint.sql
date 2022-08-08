


drop table if exists history_bigint;

CREATE TABLE `history_bigint` (
  `AUTO_ID` bigint(13) NOT NULL AUTO_INCREMENT,
  `DEL_ID` int(11) DEFAULT NULL,
  `ROW_ID` int(11) DEFAULT NULL,
  `TARGET` varchar(75) NOT NULL,
  `IP_ADDRESS` varchar(75) DEFAULT NULL,
  `RESOURCE_ID` int(11) DEFAULT NULL,
  `NOTES` mediumtext,
  `ENTRY_DATE` datetime DEFAULT NULL,
  `ARCHIVE_FLAG` char(1) DEFAULT 'N',
  `DEL_FLAG` char(1) DEFAULT 'N',
  `CREATE_DATE` datetime DEFAULT NULL,
  `USER_TIME_ZONE` varchar(10) DEFAULT NULL,
  `TZ_ID` varchar(150) DEFAULT NULL,
  `UPDATE_DATE` datetime DEFAULT NULL,
  `AUTO_UPDATE_DATE` datetime DEFAULT NULL,
  `AUTO_UPDATE_SRC` varchar(100) DEFAULT NULL,
  PRIMARY KEY (`AUTO_ID`),
  KEY `historyROW_ID_I0` (`ROW_ID`),
  KEY `historyTARGET_I1` (`TARGET`)
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;

SET @seed = (SELECT CEILING(MAX(AUTO_ID) / 1000000) * 1000000 FROM history) + 100000000;
SET @alter_seed = CONCAT('ALTER TABLE `history_bigint` AUTO_INCREMENT = ', @seed);

PREPARE stmt FROM @alter_seed;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;



